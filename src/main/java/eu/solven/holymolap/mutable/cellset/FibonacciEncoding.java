package eu.solven.holymolap.mutable.cellset;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

// https://www.geeksforgeeks.org/fibonacci-coding/
public class FibonacciEncoding {
	private static final Logger LOGGER = LoggerFactory.getLogger(FibonacciEncoding.class);

	// The 45th Fibonacci number does not fit inside an int
	public static int N = 46;

	// Array to store fibonacci numbers.
	// fib[i] is going to store (i+2)'th
	// Fibonacci number
	public static int[] fib = new int[N];

	static {
		initFibo(Integer.MAX_VALUE);
	}

	// Stores values in fib and returns index of
	// the largest fibonacci number smaller than n.
	private static int initFibo(int n) {
		// Fib[0] stores 2nd Fibonacci No.
		fib[0] = 1;

		// Fib[1] stores 3rd Fibonacci No.
		fib[1] = 2;

		// Keep Generating remaining numbers while
		// previously generated number is smaller
		int i;
		for (i = 2; fib[i - 1] <= n && i < N; i++) {
			int sum = fib[i - 1] + fib[i - 2];
			if (sum < 0) {
				// We overflown
				i = i - 1;
				break;
			}
			fib[i] = sum;
		}

		LOGGER.info("We stopped at {}={}+{}", fib[i], fib[i - 1], fib[i - 2]);
		return i;
	}

	public static int largestFiboLessOrEqual(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Fibonacci encoding handles inly striclty positive integers");
		}

		int index = indexofLargestFibonacciLessOrEquals(n);

		return fib[index];
	}

	public static int indexofLargestFibonacciLessOrEquals(int n) {
		// Return index of the largest fibonacci
		// number smaller than or equal to n.
		int index = Arrays.binarySearch(fib, n);
		if (index < 0) {
			return -(index + 1) - 1;
		} else {
			// Equals
			return index;
		}
	}

	// Returns pointer to the char string which
	// corresponds to code for n
	public static String fibonacciEncoding(int n) {
		if (n < 1) {
			throw new IllegalArgumentException("Fibonacci encoding handles only striclty positive integers");
		}

		int index = indexofLargestFibonacciLessOrEquals(n);

		// Allocate memory for codeword
		char[] codeword = new char[index + 2];

		// Index of the largest Fibonacci f <= n
		int i = index;

		while (n > 0) {

			// Mark usage of Fibonacci f(1 bit)
			codeword[i] = '1';

			// Subtract f from n
			n = n - fib[i];
			assert n >= 0;

			// Move to Fibonacci just smaller than f
			i = i - 1;

			// Mark all Fibonacci > n as not used
			// (0 bit), progress backwards
			while (i >= 0 && fib[i] > n) {
				codeword[i] = '0';
				i = i - 1;
			}
		}

		// Additional '1' bit
		codeword[index + 1] = '1';
		String string = new String(Arrays.copyOf(codeword, index + 2));

		// Return pointer to codeword
		return string;
	}

	public static long fibonacciEncodingToLong(int n) {
		return fibonacciEncodingToLong(n, 0L, 64 - 1);
	}

	public static long fibonacciEncodingToLong(int n, long codeword, int shift) {
		if (n < 1) {
			throw new IllegalArgumentException("Fibonacci encoding handles inly striclty positive integers");
		}

		int index = indexofLargestFibonacciLessOrEquals(n);

		// Index of the largest Fibonacci f <= n
		int i = index;

		while (n > 0) {

			// Mark usage of Fibonacci f(1 bit)
			// codeword[i] = '1';
			codeword ^= (1L << (shift - i));

			// Subtract f from n
			n = n - fib[i];
			assert n >= 0;

			// Move to Fibonacci just smaller than f
			i = i - 1;

			// Mark all Fibonacci > n as not used
			// (0 bit), progress backwards
			while (i >= 0 && fib[i] > n) {
				// codeword[i] = '0';
				i = i - 1;
			}
		}

		// Additional '1' bit
		// codeword[index + 1] = '1';
		codeword ^= (1L << (shift - (index + 1)));

		return codeword;
	}

	// Driver code
	public static void main(String[] args) {
		int n = 143;

		System.out.println("Fibonacci code word for " + n + " is " + fibonacciEncoding(n));
	}

	public static long fibonacciEncodingToLong(int[] ints) {
		return fibonacciEncodingToLong(IntArrayList.wrap(ints));
	}

	public static long fibonacciEncodingToLong(IntList ints) {
		long codeword = 0L;
		int shift = 63;

		int inputSize = ints.size();
		for (int intIndex = 0; intIndex < inputSize; intIndex++) {
			int oneInt = ints.getInt(intIndex);
			if (oneInt == Integer.MAX_VALUE) {
				throw new IllegalArgumentException("We do not accept Integer.MAX_VALUE in the array");
			} else if (oneInt < 0) {
				throw new IllegalArgumentException("We do not accept negative integers in the array");
			}

			// We accept 0, by shifting all values by 1
			int n = oneInt + 1;

			int index = indexofLargestFibonacciLessOrEquals(n);

			// Index of the largest Fibonacci f <= n
			int i = index;

			while (n > 0) {

				// Mark usage of Fibonacci f(1 bit)
				// codeword[i] = '1';
				codeword ^= (1L << (shift - i));
				// System.out.println(longToBinaryWithLeading(codeword));

				// Subtract f from n
				n = n - fib[i];
				assert n >= 0;

				// Move to Fibonacci just smaller than f
				i = i - 1;

				// Mark all Fibonacci > n as not used
				// (0 bit), progress backwards
				while (i >= 0 && fib[i] > n) {
					// codeword[i] = '0';
					i = i - 1;
				}
			}

			// Additional '1' bit
			// codeword[index + 1] = '1';
			codeword ^= (1L << (shift - (index + 1)));
			// System.out.println(longToBinaryWithLeading(codeword));

			shift -= index + 2;
		}
		return codeword;
	}

	public static String longToBinaryWithLeading(long longs) {
		return String.format("%64s", Long.toBinaryString(longs)).replace(' ', '0');
	}
}
