/**
 * 
 */
package org.stevemaurer.opensource.mhs;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * JUnit - Correctness and performance tests for MTSort
 * 
 * These tests also log performance results to the console.
 * 
 * @author Steve Maurer
 */
public class MTSortTest {
	
	Integer[] sortedArray;
	Integer[] unsortedArray;
	static private ThreadPoolExecutor mThreadPool;

	
	public static final int kArrayLength = 0x100000;
	
	// Randomize an array. 
	private void randomize(int length)
	{
		Random rand = new Random();
//		long seed = rand.nextLong();
//		System.out.println("Seed=" + seed);	// Print the seed, so the test can be reproduced if ever there is a problem
//		rand = new Random(seed);
		for (int i = 0; i < length; i++ )
			unsortedArray[i] =  rand.nextInt();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		
		final int availableProcessors = Runtime.getRuntime().availableProcessors();

		mThreadPool = new ThreadPoolExecutor(availableProcessors, availableProcessors, 10,
							TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(availableProcessors));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		sortedArray = new Integer[ kArrayLength ];
		unsortedArray = new Integer[ kArrayLength ];
		randomize(unsortedArray.length);
		System.arraycopy(unsortedArray, 0, sortedArray, 0, unsortedArray.length);
	}

	/**
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, T[])}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArray() throws InterruptedException
	{
			// Sort the array using the standard utility library. Keep the elapsed time.
		Arrays.sort(sortedArray);
		
			// Sort the array using the multithreaded library
		MTSort.MTsort(mThreadPool, unsortedArray);

		// Test for correctness.Everything else is performance related.
		int i = 0;
		for ( Integer elem : unsortedArray )
		{
			if ( !sortedArray[i].equals(elem) )
				fail("Sort failed at item #" + i + "  mt=" + elem + "  normal=" + sortedArray[i]);
			
			i++;
		}

	}
	
	/**
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, T[], int, int, int, Comparator<? super T> compar)}.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntIntCompar() throws InterruptedException {
		
		// Make a Comparator that uses the reverse of the natural ordering.
		Comparator<Integer> compFunc = new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return o2.compareTo(o1);
			}
		};
		
		// Sort using the comparator function
		Arrays.sort(sortedArray, compFunc);
		
		// Sort multithreaded using the comparator function
		MTSort.MTsort(mThreadPool, unsortedArray, 0, unsortedArray.length, mThreadPool.getMaximumPoolSize(), compFunc);
		
		// Check for equality
		int i = 0;
		for ( Integer elem : unsortedArray )
		{
			if ( !sortedArray[i].equals(elem) )
				fail("Sort failed at item #" + i + "  mt=" + elem + "  normal=" + sortedArray[i]);
			
			i++;
		}
	}

	/**
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, T[], int, int, int)}.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntInt_PerformanceAnalysisNatural() throws InterruptedException {

		int expandedArrayLength = kArrayLength * 4;
		sortedArray = new Integer[ expandedArrayLength ];
		unsortedArray = new Integer[ expandedArrayLength ];
		
		System.out.println("Natural ordering:");
		
		for ( int len = 0x1000; len <= expandedArrayLength; len <<= 1)
		{
			randomize(len);
			System.arraycopy(unsortedArray, 0, sortedArray, 0, len);
			
			long elapsedSortTime = -System.currentTimeMillis();
			Arrays.sort(sortedArray, 0, len);
			elapsedSortTime += System.currentTimeMillis();
			
			long mtElapsedSortTime = -System.currentTimeMillis();
			MTSort.MTsort(mThreadPool, unsortedArray, 0, len);
			mtElapsedSortTime += System.currentTimeMillis();
			
			System.out.print("Size=" + len + "\tArrays.sort() time= " + elapsedSortTime + "mS    \t-- ");
			System.out.println("MTSort.Mtsort(x" + mThreadPool.getMaximumPoolSize() + ") time= " + mtElapsedSortTime + "mS");
		}
	}

	/**
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, T[], int, int, int)}.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntInt_PerformanceAnalysisComparator() throws InterruptedException {

		int expandedArrayLength = kArrayLength * 4;
		sortedArray = new Integer[ expandedArrayLength ];
		unsortedArray = new Integer[ expandedArrayLength ];
		
		// Make a Comparator that uses the reverse of the natural ordering.
		Comparator<Integer> compFunc = new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return o2.compareTo(o1);
			}
		};
		
		System.out.println("Comparator ordering:");
		for ( int len = 0x1000; len <= expandedArrayLength; len <<= 1)
		{
			randomize(len);
			System.arraycopy(unsortedArray, 0, sortedArray, 0, len);
			
			long elapsedSortTime = -System.currentTimeMillis();
			Arrays.sort(sortedArray, 0, len, compFunc);
			elapsedSortTime += System.currentTimeMillis();
			
			long mtElapsedSortTime = -System.currentTimeMillis();
			int threads = mThreadPool.getMaximumPoolSize();
			MTSort.MTsort(mThreadPool, unsortedArray, 0, len, threads, compFunc);
			mtElapsedSortTime += System.currentTimeMillis();
			
			System.out.print("Size=" + len + "\tArrays.sort() time= " + elapsedSortTime + "mS    \t-- ");
			System.out.println("MTSort.Mtsort(x" + threads + ") time= " + mtElapsedSortTime + "mS");
		}
	}
}
