package eu.solven.holymolap.mutable.cellset;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;

// https://www.geeksforgeeks.org/fibonacci-coding/
// https://en.wikipedia.org/wiki/Zeckendorf%27s_theorem
// https://en.wikipedia.org/wiki/Fibonacci_coding
public class FibonacciEncodingCodec implements ByteIntegerCODEC {
	private static final Logger LOGGER = LoggerFactory.getLogger(FibonacciEncodingCodec.class);

	final int minValue;

	/**
	 * FibonacciEncoding accepts only positive integers. We can handle any range of integers by shifting them (e.g. +1
	 * to handle 0).
	 * 
	 * We may also consider https://en.wikipedia.org/wiki/Negafibonacci_coding
	 * 
	 * @param minValue
	 */
	public FibonacciEncodingCodec(int minValue) {
		this.minValue = minValue;
	}

	@Override
	public void compress(int[] in, IntWrapper inpos, int inlength, byte[] out, IntWrapper outpos) {
		compress(IntArrayList.wrap(in), inpos, inlength, out, outpos);
	}

	/**
	 * Creates a new buffer of the requested size.
	 *
	 * In case you need a different way to allocate buffers, you can override this method with a custom behavior. The
	 * default implementation allocates a new Java direct {@link ByteBuffer} on each invocation.
	 */
	protected ByteBuffer makeBuffer(int sizeInBytes) {
		return ByteBuffer.allocateDirect(sizeInBytes);
	}

	private static final int bitsPerByte = 8;
	// 0x1000000
	private static final byte firstBit = Byte.MIN_VALUE;
	// 0x0111111
	private static final byte notFirstBit = ~firstBit;

	// https://stackoverflow.com/questions/3948220/behaviour-of-unsigned-right-shift-applied-to-byte-variable
	// https://stackoverflow.com/questions/19181411/circular-rotate-issue-with-rotate-left
	public static byte rotateRight(byte bits, int shift) {
		return (byte) (((bits & 0xff) >>> shift) | ((bits & 0xff) << (bitsPerByte - shift)));
	}

	// https://stackoverflow.com/questions/19181411/circular-rotate-issue-with-rotate-left
	public static byte rotateLeft(byte bits, int shift) {
		return (byte) (((bits & 0xff) << shift) | ((bits & 0xff) >>> (bitsPerByte - shift)));
	}

	public void compress(IntList ints, IntWrapper inpos, int inlength, byte[] out, IntWrapper outpos) {
		if (inpos == outpos) {
			throw new IllegalArgumentException("inputPosition and outputPosition must be different references");
		}

		int maxAllowed;
		if (minValue > 0) {
			// An int can not be greated than this value
			maxAllowed = Integer.MAX_VALUE;
		} else {
			// This will be lower than Integer.MAX_VALUE
			maxAllowed = Integer.MAX_VALUE + minValue;
		}

		// We start writing at the first bit of the N-th byte
		int bitShift = 0;
		int byteIndex = outpos.get();

		for (int k = inpos.get(); k < inpos.get() + inlength; ++k) {
			int oneInt = ints.getInt(k);
			if (oneInt >= maxAllowed) {
				throw new IllegalArgumentException(
						"We do not accept integers above " + maxAllowed + " in the array: " + ints);
			} else if (oneInt < minValue) {
				throw new IllegalArgumentException(
						"We do not accept integers below " + minValue + " in the array: " + ints);
			}

			// We accept 0, by shifting all values by 1
			int n = oneInt + 1 - minValue;
			assert n > 0;

			int index = FibonacciEncoding.indexofLargestFibonacciLessOrEquals(n);

			// Index of the largest Fibonacci f <= n
			int i = index;

			do {
				// Mark usage of Fibonacci f(1 bit)
				// codeword[i] = '1';
				int bitIndex3 = bitShift + i;
				out[byteIndex + bitIndex3 / bitsPerByte] |= rotateRight(firstBit, bitIndex3 % bitsPerByte);
				// System.out.println(longToBinaryWithLeading(codeword));

				// Subtract f from n
				n = n - FibonacciEncoding.fib[i];
				assert n >= 0;

				// Move to Fibonacci just smaller than f
				i = i - 1;

				// Mark all Fibonacci > n as not used
				// (0 bit), progress backwards
				while (i >= 0 && FibonacciEncoding.fib[i] > n) {
					// codeword[i] = '0';
					int bitIndex4 = bitShift + i;
					out[byteIndex + bitIndex4 / bitsPerByte] &= rotateRight(notFirstBit, bitIndex4 % bitsPerByte);

					i = i - 1;
				}
			} while (n > 0);

			// Additional '1' bit
			// codeword[index + 1] = '1';
			int bitIndex2 = bitShift + index + 1;
			out[byteIndex + bitIndex2 / bitsPerByte] |= rotateRight(firstBit, bitIndex2 % bitsPerByte);
			// System.out.println(longToBinaryWithLeading(codeword));

			// int nbBits = index + 2;

			bitShift += index + 2;
			int nbBytes = bitShift / bitsPerByte;
			// int remainder = bitShift - (nbBytes * bitsPerByte);
			bitShift -= nbBytes * bitsPerByte;

			byteIndex += nbBytes;
		}

		// Append zeroes to the last byte.
		if (bitShift > 0) {
			for (int trailBitShift = bitShift; trailBitShift < bitsPerByte; trailBitShift++) {
				out[byteIndex] &= rotateRight(notFirstBit, trailBitShift);
			}

			// Set as outPosition the next writable byteIndex
			outpos.set(byteIndex + 1);
		} else {
			// bitShift==0 means we fully used the last byte
			outpos.set(byteIndex);
		}

		inpos.add(inlength);
	}

	@Override
	public void uncompress(byte[] in, IntWrapper inpos, int inlength, int[] out, IntWrapper outpos) {
		throw new UnsupportedOperationException("TODO. Please report your use-case");
	}

}
