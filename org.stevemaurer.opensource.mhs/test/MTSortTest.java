/**
 * 
 */

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
import org.stevemaurer.opensource.mhs.MTSort;

/*
 * JUnit - Correctness and performance tests for MTSort
 * These tests also log performance results to the console.
 * 
 * @author Steve Maurer
 */
@SuppressWarnings({"RedundantThrows", "SpellCheckingInspection"})
public class MTSortTest {
	
	Integer[] sortedArray;
	Integer[] unsortedArray;
	static private ThreadPoolExecutor mThreadPool;

	
	public static final int kArrayLength = 0x400000;
	
	// Randomize an array. 
	private void randomize(int length)
	{
		Random rand = new Random();

		// In theory, you can to a nextLong(), print the seed, and recreate a repeatable Random if there ever is
		// a correctness problem here.
		for (int i = 0; i < length; i++ )
			unsortedArray[i] =  rand.nextInt();
	}

	/*
	 * @throws java.lang.Exception -- If OS fails to allocate a resource
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		
		final int availableProcessors = Runtime.getRuntime().availableProcessors();

		mThreadPool = new ThreadPoolExecutor(availableProcessors, availableProcessors, 10,
							TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(availableProcessors));
	}

	/*
	 * @throws java.lang.Exception  -- If OS fails to allocate a resource
	 */
	@Before
	public void setUp() throws Exception {

		sortedArray = new Integer[ kArrayLength ];
		unsortedArray = new Integer[ kArrayLength ];
		randomize(unsortedArray.length);
		System.arraycopy(unsortedArray, 0, sortedArray, 0, unsortedArray.length);
	}

	/*
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, java.lang.Comparable[])}.
	 * @throws InterruptedException -- If test is manually interrupted
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
	
	/*
	 * Test method for {@link MTSort#MTsort(ThreadPoolExecutor, Comparable[], int, int, int, Comparator)}.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntIntCompar() throws InterruptedException {
		
		// Make a Comparator that uses the reverse of the natural ordering.
		Comparator<Integer> compFunc = Comparator.reverseOrder();
		
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

	/*
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, java.lang.Comparable[], int, int, int)}.
	 * It uses the built-in "natural" ordering code.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntInt_PerformanceAnalysisNatural() throws InterruptedException {

		int expandedArrayLength = kArrayLength * 4;
		sortedArray = new Integer[ expandedArrayLength ];
		unsortedArray = new Integer[ expandedArrayLength ];
		
		System.out.println("Natural ordering:");

		// Loop through various sorting lengths, starting at 0x1000 and doubling until we reach the array size
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
			final int threads = Math.min(mThreadPool.getMaximumPoolSize(), Runtime.getRuntime().availableProcessors());

			System.out.printf("Size=%-10d\tArrays.sort() time=%-10s", len, elapsedSortTime +"mS");
			System.out.printf("-- MTSort.Mtsort(x%d) time=%dmS\n", threads, mtElapsedSortTime);
		}
	}

	/*
	 * Test method for {@link org.stevemaurer.opensource.mhs.MTSort#MTsort(java.util.concurrent.ThreadPoolExecutor, java.lang.Comparable[], int, int, int, java.util.Comparator)}.
	 */
	@Test
	public void testMTsortThreadPoolExecutorTArrayIntIntInt_PerformanceAnalysisComparator() throws InterruptedException {

		int expandedArrayLength = kArrayLength * 4;
		sortedArray = new Integer[ expandedArrayLength ];
		unsortedArray = new Integer[ expandedArrayLength ];
		
		// Make a Comparator that uses the reverse of the natural ordering.
		Comparator<Integer> compFunc = Comparator.reverseOrder();
		
		System.out.println("Comparator ordering:");

		// Loop through various sorting lengths, starting at 0x1000 and doubling until we reach the array size
		for ( int len = 0x1000; len <= expandedArrayLength; len <<= 1)
		{
			randomize(len);
			System.arraycopy(unsortedArray, 0, sortedArray, 0, len);
			
			long elapsedSortTime = -System.currentTimeMillis();
			Arrays.sort(sortedArray, 0, len, compFunc);
			elapsedSortTime += System.currentTimeMillis();
			
			long mtElapsedSortTime = -System.currentTimeMillis();
			final int threads = mThreadPool.getMaximumPoolSize();
			MTSort.MTsort(mThreadPool, unsortedArray, 0, len, threads, compFunc);
			mtElapsedSortTime += System.currentTimeMillis();
			
			System.out.printf("Size=%-10d\tArrays.sort() time=%-10s", len, elapsedSortTime +"mS");
			System.out.printf("-- MTSort.Mtsort(x%d) time=%dmS\n", threads, mtElapsedSortTime);
		}
	}
}
