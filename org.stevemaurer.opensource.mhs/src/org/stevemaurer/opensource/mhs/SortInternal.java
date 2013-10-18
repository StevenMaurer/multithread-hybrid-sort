package org.stevemaurer.opensource.mhs;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * DESCRIPTION:
 * An internal container class for a blazingly fast multithreaded hybrid sort.
 * This class is package local. Use it through its external interface MTSort.java
 * 
 * DESIGN NOTES:
 * This is really just a front end for java's built-in Arrays.sort(), which is a very elegant and fast 
 * heap sort. That class's lone major issue is it's lack of multithreading, which is corrected here.
 *
 * This is done by borrowing quick-sort's partitioning, subdividing each partition to sort in its own thread.
 * When a partition becomes too small or it runs out of threads, we fall back on the built-in. This hybrid
 * approach is naturally multithreaded. There are no synchronize() statements (or contention) at all because
 * each subdivision is entirely owned by each thread.
 * 
 * Callers can still get in trouble if their element <T>'s hashCode() performs locking that is used by some
 * other task that is scheduled on the (presumably shared) threadpool handed to this library. As a contrived
 * example, imagine an Runnable that grabs a lock A then B, and a hashCode() of <T> that grabs lock B then
 * A to return a value. This would normally work in a regular sort running in the same context as the Runnable,
 * but could have a deadlock if both Runnables were put on the same pool.Not expected to be a common situation.
 * 
 * The sort can also run into performance issues if the threadpool is entirely clogged. But again, unlikely.
 * The efficiency of the sorting (vs non-multithreaded) can go down in large arrays due to memory contention; try
 * to keep things in L3 cache, if possible.
 * 
 * SortInternal's purpose is to simply hold the static values for the various SubSort runnable class instances.
 * There are also no methods to SortInternal. Instancing it kicks off the sort itself.
 * (yes, this can be terrible practice - Bad Programmer, Bad! No cheetos! - however it simply looks cleaner)
 * 
 * @author Steve Maurer
 *
 * @param <T>
 */
final class SortInternal<T extends Comparable<T>>
{
	private final Semaphore				mDoneSem;			// Count semaphore indicating process is done
	private final ThreadPoolExecutor	mPool;				// Pool of executors
	private final T[]					mArray;				// Array to be sorted
	private final Comparator<? super T>	mCompar;			// Array element comparator; null for natural order 
	
		// Size at which further subdivision isn't worth the overhead. The value hardly matters: for any decent
		// sized array, you will never hit this. Further, empirical experimentation has shown that threadpools
		// are so efficient that there is almost no point where the overhead is not worth using another thread.
		// The initial design allowed users to control this, but this was unnecessarily cluttered.
		// The value is now set to a hard-coded 0x100 (256).
	private static final int			kMinSegmentSize = 0x100;
	
	/**
	 * Sort in a multithreaded fashion recursively.
	 * 
	 * Sort is NOT done when this class is instanced. Rather, it is done when "permits" number of permits
	 * are posted to doneSem - i.e. when doneSem.acquire(permits) returns. See MTSort.java.
	 * 
	 * @param doneSem		Semaphore that indicates the sort is done
	 * @param permits		Number of permits that will be be posted on doneSem; each represents a thread context
	 * @param pool			Caller's threadpool to use
	 * @param array			Array to be sorted
	 * @param base			Base of range to sort
	 * @param onePastEnd	End of range to sort (exclusive)
	 * @param compar		Comparator (null to use natural ordering)
	 */
	public SortInternal(Semaphore doneSem, int permits, ThreadPoolExecutor pool, T[] array, int base, int onePastEnd, Comparator<? super T> compar)
	{
			// First, initialize all our final values for use by all the SubSort instances
		mDoneSem = doneSem;
		mPool = pool;
		mArray = array;
		mCompar = compar;
		
			// Next, kick off our top level sort. Execute initial sort in the caller's context.
		final SubSort internalSorter = new SubSort(permits, base, onePastEnd);
		internalSorter.run();
	}

	/**
	 * Sort a portion of mArray from mBase to mOnePastEnd (exclusive)
	 * It may create up to "mPermits" SubSort threads to accomplish this task.
	 * 
	 * @author Steve Maurer
	 */
	private final class SubSort implements Runnable
    {
    	int mPermits;				// The number of thread permits this sub-sort is allowed
    	final int mBase;			// Base index of mArray to be sorted
    	int mOnePastEnd;			// Top index of mArray (exclusive) to be sorted

    	// POJO Constructor
    	private SubSort(int permits, int base, int onePastEnd)
    	{
    		if (permits <= 0)
    			throw new IllegalArgumentException();
    		mPermits = permits;
    		mBase = base;
    		mOnePastEnd = onePastEnd;
    	}

    	/**
    	 * The main sorting execution thread of mArray across 'base' (inclusive) to 'mOnePastEnd' (exclusive).
    	 */
		@Override
		public void run()
		{
			/**
			 * Enclose everything in a for-loop used for tail-recursion
			 */
			for (;;)
			{
				/**
				 * If we are at a leaf of the thread permits, or our segment size has grown small enough that the
				 * overhead is considered not worth it, fall back on sorting this section with java's mergesort.
				 */
				if ( mPermits == 1 || mOnePastEnd < mBase + kMinSegmentSize )
				{
					Arrays.sort(mArray, mBase, mOnePastEnd, mCompar);
					mDoneSem.release(mPermits);
					return;
				}
				
				/**
				 * Select a (hopefully) median pivot and partition all elements in place so that all values
				 * with indexes lowe than the pivot compare less to the pivot, and all in higher indexes
				 * compare greater than the pivot value.
				 * 
				 * Use partitionCompar() or partitionNatural() depending on if mCompar is non-null.
				 */
				final int segmentLen = mOnePastEnd - mBase;
				final int pivotIdx = mCompar!=null ? partitionCompar() : partitionNatural();
				
				/**
				 * Provision thread permits between the two partitions proportionally based on the pivot location
				 * 
				 * In other words, if our sort ended like this:
				 * 
				 * [ lo lo lo lo <PIVOT> hi hi hi hi hi hi hi hi hi hi hi hi hi hi hi hi hi hi hi ]
				 * 
				 * ...make sure that most of the thread permits go to work on the "hi" section.
				 */
				int firstPartitionPermits = Math.round((float)mPermits * (pivotIdx-mBase) / segmentLen); 
	
				if ( firstPartitionPermits == 0 ) // Ensure each side has at least one permit
					firstPartitionPermits = 1;
				else if ( firstPartitionPermits == mPermits )
					firstPartitionPermits--;
				
				/**
				 * Schedule each partition to be sorted in its own thread.
				 */
				mPool.execute(new SubSort(mPermits-firstPartitionPermits, pivotIdx, mOnePastEnd));
	
				/**
				 * For the first partition, use tail-recursion to reuse this thread and avoid overhead and stack depth.
				 */
				mPermits = firstPartitionPermits;
				mOnePastEnd = pivotIdx;
			}
		}
		
		/**
		 * Partition mArray based on the natural ordering of <T>
		 * 
		 * @return Index location of the selected pivot
		 */
		private final int partitionNatural()
		{
			/**
			 * Divide the range into three sections and select points p1, p2, & p3, from the center of each.
			 * This is an attempt to avoid outliers which may be stuck on the ends of the array.
			 */
			final int segmentLen = mOnePastEnd - mBase;
			final T p1 = mArray[mBase + segmentLen/6],	p2 = mArray[mBase + segmentLen/2], p3 = mArray[mBase + (int)((5L*segmentLen)/6)];
			
			/**
			 * Select the pivot by figuring out which is the median of p1, p2, & p3. Code is optimized so that
			 * only two comparisons are needed to determine that p2 is the median.
			 * 
			 * Stylewise, use neither inscrutable extended ternary conditionals, nor superfluous parenthesis.
			 */
			final T pivot;
			if ( p1.compareTo(p2) > 0 )
				if ( p2.compareTo(p3) > 0 )			// p1 > p2 && p2 > p3  				means p2 is pivot
					pivot = p2;
				else if ( p1.compareTo(p3) > 0 )	// p1 > p2 && p2 < p3 && p1 > p3	means p3 is pivot
					pivot = p3;
				else								// p1 > p2 && p2 < p3 && p1 <= p3	means p1 is pivot
					pivot = p1;	
			else if ( p2.compareTo(p3) <= 0 )		// p1 <= p2 && p2 <= p3				means p2 is pivot
				pivot = p2;
			else if ( p1.compareTo(p3) > 0 )		// p1 <= p2 && p2 > p3 && p1 > p3	means p1 is pivot
				pivot = p1;
			else									// p1 <= p2 && p2 > p3 && p1 <= p3	means p3 is pivot
				pivot = p3;
			
			/**
			 * Perform the actual partition, using the natural ordering
			 */
			for (int lo = mBase, hi = mOnePastEnd-1; ; lo++, hi--)
			{					
					// Skip until we find an item above the pivot in the lower section
				while ( pivot.compareTo(mArray[lo]) >= 0 )
					lo++;

					// Skip until we find an item below the pivot in the upper section
				while ( pivot.compareTo(mArray[hi]) <= 0 )
					hi--;
				
					// If we've looked at everything, we're done
				if ( lo >= hi )
					return lo;
				
					// Swap the two identified entries, putting them in their correct sections 
				final T tmp = mArray[lo];
				mArray[lo] = mArray[hi];
				mArray[hi] = tmp;
			}
		}
		
		/**
		 * Partition mArray based on mCompar()
		 * 
		 * @return Index location of the selected pivot
		 */
		private final int partitionCompar()
		{
			/**
			 * Divide the range into three sections and select points p1, p2, & p3, from the center of each.
			 * This is an attempt to avoid outliers which may be stuck on the ends of the array.
			 */
			final int segmentLen = mOnePastEnd - mBase;
			final T p1 = mArray[mBase + segmentLen/6],	p2 = mArray[mBase + segmentLen/2], p3 = mArray[mBase + (int)((5L*segmentLen)/6)];
			
			/**
			 * Select the pivot by figuring out which is the median of p1, p2, & p3. Code is optimized so that
			 * only two comparisons are needed to determine that p2 is the median.
			 * 
			 * Stylewise, use neither inscrutable extended ternary conditionals, nor superfluous parenthesis.
			 */
			final T pivot;
			if (  mCompar.compare(p1, p2) > 0 )
				if (  mCompar.compare(p2, p3) > 0 )			// p1 > p2 && p2 > p3  				means p2 is pivot
					pivot = p2;
				else if (  mCompar.compare(p1, p3) > 0 )	// p1 > p2 && p2 < p3 && p1 > p3	means p3 is pivot
					pivot = p3;
				else										// p1 > p2 && p2 < p3 && p1 <= p3	means p1 is pivot
					pivot = p1;	
			else if (  mCompar.compare(p2, p3) <= 0 )		// p1 <= p2 && p2 <= p3				means p2 is pivot
				pivot = p2;
			else if (  mCompar.compare(p1, p3) > 0 )		// p1 <= p2 && p2 > p3 && p1 > p3	means p1 is pivot
				pivot = p1;
			else											// p1 <= p2 && p2 > p3 && p1 <= p3	means p3 is pivot
				pivot = p3;
			
			/**
			 * Perform the actual partition, using mCompar to do the ordering
			 */
			for (int lo = mBase, hi = mOnePastEnd-1; ; lo++, hi--)
			{					
					// Skip until we find an item above the pivot in the lower section
				while ( mCompar.compare(pivot, mArray[lo]) >= 0 )
					lo++;

					// Skip until we find an item below the pivot in the upper section
				while ( mCompar.compare(pivot, mArray[hi]) <= 0 )
					hi--;
				
					// If we've looked at everything, we're done
				if ( lo >= hi )
					return lo;
				
					// Swap the two identified entries, putting them in their correct sections 
				final T tmp = mArray[lo];
				mArray[lo] = mArray[hi];
				mArray[hi] = tmp;
			}
		}
    }
}
    