package org.stevemaurer.opensource.mhs;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;


/*
 * DESCRIPTION:
 * A blazingly fast in-place multithreaded sort that uses dependency injection of a ThreadPoolExecutor pool
 * 
 * DESIGN NOTES:
 * See the SortInternal class for detailed design notes, however briefly:
 * - As POJO utility class, using Spring (and/or JSR-330) annotations is overkill; downrev JRE compatibility is more important
 * - Users must inject the threadpool, as allocating pools on the fly misses their point completely.
 * - This isn't some school demo project. It falls back on Java's fast built-in Arrays.sort() in the end.
 * - It is utterly thread-safe/reentrant, but uses no synchronize statements (outside those in the counting sem).
 * - The default number of threads used is limited to the number of processors on the system.
 * - It is the caller's responsibility to ensure the threadpool is in working order
 * 
 * All methods in this class are themselves thread-safe.
 * 
 * @author Steve Maurer
 */
public final class MTSort
{
	/**
	 * MTSort - Sort T[] array using ThreadPoolExecutor pool.
	 * 
	 * @param pool						Thread pool
	 * @param array						Input array to sort
	 * @throws InterruptedException		If the sort was interrupted by the operator
	 */
    public static <T extends Comparable<T>> void MTsort(final ThreadPoolExecutor pool, final T[] array)
    	throws InterruptedException
    {
    	MTsort(pool, array, 0, array.length);
    }
    
    /*
	 * MTSort - Sort T[] array using ThreadPoolExecutor pool.
	 * 
	 * @param pool						Thread pool
	 * @param array						Input array to sort
	 * @param base						First element of the array to sort
	 * @param onePastEnd				One element past the end of the array to sort
	 * @throws InterruptedException		If the sort was interrupted by the operator
	 */
    public static <T extends Comparable<T>> void MTsort(final ThreadPoolExecutor pool, final T[] array, int base, int onePastEnd)
    	throws InterruptedException
    {
    		// Number of threads to use is the lesser of pool size and system processors
    		// Additional threads past the number of system processors don't get you anything (by default)
    	final int permits = Math.min(pool.getMaximumPoolSize(), Runtime.getRuntime().availableProcessors());
    	
    	MTsort(pool, array, base, onePastEnd, permits, null);
    }
    
    /*
     * MTSort - Sort T[] array using ThreadPoolExecutor pool.
     * 
     * This class offers control over the section size and number of thread permits.
     * This lets you fiddle with scenarios outside in-mem-only sorts. With great power comes great responsibility.
     * 
     * @param pool						The thread pool to use
     * @param array						The array of elements to sort
     * @param base						The base of the range to sort (inclusive)
     * @param onePastEnd				End of range to sort (exclusive)
     * @param permits					The maximum number of simultaneous threads the sort will use. Clamped > 1.
     * @throws InterruptedException		If the sort was interrupted by the operator
     */
    public static <T extends Comparable<T>> void MTsort(final ThreadPoolExecutor pool,
    		final T[] array, final int base, final int onePastEnd, int permits)
    	throws InterruptedException
    {
    	MTsort(pool, array, base, onePastEnd, permits, null);
    }
    
    /*
     * MTSort - Sort T[] array using ThreadPoolExecutor pool.
     * 
     * This class offers control over the section size and number of thread permits.
     * This lets you fiddle with scenarios outside in-mem-only sorts. With great power comes great responsibility.
     * 
     * @param pool						The thread pool to use
     * @param array						The array of elements to sort
     * @param base						Base of range to sort (inclusive)
     * @param onePastEnd				End of range to sort (exclusive)
     * @param permits					The maximum number of simultaneous threads the sort will use.
     * @param comparator				Comparator to determine the order of the array. Null means use natural ordering.
     * @throws InterruptedException		If the sort was interrupted by the operator
     */
    public static <T extends Comparable<T>> void MTsort(final ThreadPoolExecutor pool,
    		final T[] array, final int base, final int onePastEnd, final int permits, final Comparator<? super T> comparator)
    	throws InterruptedException
    {
    		// Throw exception on actual out-of-bounds condition
    	if ( base < 0 || onePastEnd > array.length )
    		throw new IllegalArgumentException("Bad bounding value (" + base + ","+onePastEnd + "). [Array size=" + array.length + "]");
    	
    		// Simple case: told to sort zero elements of the provided array
		if ( onePastEnd - base <= 1 )
			return;
		
			// If told to execute in single-threaded manner, do so
		if ( permits <= 1 )
		{
			Arrays.sort(array, base, onePastEnd, comparator);
			return;
		}
    	
    		// Allocate a semaphore to wait on
    	final Semaphore doneSem = new Semaphore(0);

    		// Make a unique instance to sort the array using the pool given. This allows multiple simultaneous sorts.
    	new SortInternal<T>(doneSem, permits, pool, array, base, onePastEnd, comparator);
    	
    		// Wait here until the sort is complete
    	doneSem.acquire(permits);
    }
}
