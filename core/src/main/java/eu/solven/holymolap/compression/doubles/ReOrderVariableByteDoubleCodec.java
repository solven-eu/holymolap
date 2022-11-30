package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import eu.solven.holymolap.mutable.cellset.FibonacciEncoding;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.longcompression.LongVariableByte;
import me.lemire.longcompression.RoaringIntPacking;

public class ReOrderVariableByteDoubleCodec implements IDoubleCodec {
	private static final int ARRANGEMENT_COMPRESSED_INTS = 16;

	private final VariableByte intCodec = new VariableByte();
	private final LongVariableByte longCodec = new LongVariableByte();

	/**
	 * 
	 * @param lessFrequentFirst
	 * @param l
	 * @return the re-order of l, in the order expressed by order. If order is (0,63), there is no change. If order is
	 *         (63,0), the bit are reversed.
	 */
	public static long reArrange(int[] order, long l) {
		assert order.length == 64;

		long reArranged = 0L;

		// System.out.println(FibonacciEncoding.longToBinaryWithLeading(l));
		for (int i = 0; i < 64; i++) {
			// Read the bit at index i
			if (0 != (l & (Long.MIN_VALUE >>> order[i]))) {
				// Write given bit in the target index
				reArranged |= Long.MIN_VALUE >>> i;
				// System.out.println(FibonacciEncoding.longToBinaryWithLeading(reArranged));
			}
		}

		return reArranged;
	}

	public static long reverseArrange(int[] order, long l) {
		assert order.length == 64;

		long reArranged = 0L;

		// System.out.println(FibonacciEncoding.longToBinaryWithLeading(l));
		for (int i = 0; i < 64; i++) {
			// Read the bit at index i
			if (0 != (l & (Long.MIN_VALUE >>> i))) {
				// Write given bit in the target index
				reArranged |= Long.MIN_VALUE >>> order[i];
				// System.out.println(FibonacciEncoding.longToBinaryWithLeading(reArranged));
			}
		}

		return reArranged;
	}

	// TODO This behave badly for double like '46.4'
	@Override
	public void compress(double[] doubles, ByteBuffer buffer) {
		long[] asLongs = new long[doubles.length];
		for (int i = 0; i < doubles.length; i++) {
			asLongs[i] = Double.doubleToRawLongBits(doubles[i]);
		}

		// https://www.timescale.com/blog/time-series-compression-algorithms-explained/
		// https://www.baeldung.com/java-xor-operator
		// https://github.com/burmanm/gorilla-tsc
		long[] asXorLongs = new long[asLongs.length];
		asXorLongs[0] = asLongs[0];

		int[] nbChanges = new int[64];
		for (int i = 1; i < asLongs.length; i++) {
			long xor = asLongs[i - 1] ^ asLongs[i];
			asXorLongs[i] = xor;
			for (int bit = 0; bit < 64; bit++) {
				if (0 != (xor & (Long.MIN_VALUE >>> bit))) {
					nbChanges[bit]++;
					// if (bit == 63) {
					// System.out.println(doubles[i-1]);
					// System.out.println(doubles[i]);
					// System.out.println(
					// FibonacciEncoding.longToBinaryWithLeading(Double.doubleToRawLongBits(asLongs[i - 1])));
					// System.out.println(
					// FibonacciEncoding.longToBinaryWithLeading(Double.doubleToRawLongBits(asLongs[i])));
					// System.out.println(
					// FibonacciEncoding.longToBinaryWithLeading(Double.doubleToRawLongBits(xor)));
					// }
				}
			}
		}

		int[] lessFrequentFirst = IntStream.range(0, 64)
				.mapToObj(i -> i)
				.sorted(Comparator.comparing(i -> nbChanges[i]))
				.mapToInt(i -> i)
				.toArray();

		// We expect the arranged longs to be generally small (to push 0 bits on the left)
		long[] asArrangedLongs = LongStream.of(asXorLongs).map(l -> reArrange(lessFrequentFirst, l)).toArray();

		// LongStream.of(asArrangedLongs).forEach(l ->
		// System.out.println(FibonacciEncoding.longToBinaryWithLeading(l)));

		IntWrapper outPosition = new IntWrapper();
		long[] compressed = new long[ARRANGEMENT_COMPRESSED_INTS / 2 + asArrangedLongs.length * 2];

		// We write the arrangement as is (no compression as it is already compressed)
		{
			// With variableByte, the compressed output of the N-th first integers (whatever the order) is always 16
			int[] compressedArrangement = new int[ARRANGEMENT_COMPRESSED_INTS];
			IntWrapper outPositionArrangement = new IntWrapper();
			intCodec.compress(lessFrequentFirst, new IntWrapper(), 64, compressedArrangement, outPositionArrangement);
			assert outPositionArrangement.get() == compressedArrangement.length;

			// long[] compressedArrangementAsLongs = new long[compressedArrangement.length / 2];
			for (int i = 0; i < ARRANGEMENT_COMPRESSED_INTS / 2; i++) {
				compressed[i] = RoaringIntPacking.pack(compressedArrangement[i * 2], compressedArrangement[i * 2 + 1]);
			}

			outPosition.add(ARRANGEMENT_COMPRESSED_INTS / 2);
		}

		longCodec.compress(asArrangedLongs, new IntWrapper(), asArrangedLongs.length, compressed, outPosition);

		LongBuffer longBuffer = buffer.asLongBuffer();
		longBuffer.put(Arrays.copyOf(compressed, outPosition.get()));

		// Transfer the position of longBuffer to byteBuffer
		buffer.position(longBuffer.position() * 8);
	}

	// @Override
	// public void compress(double[] in, IntWrapper inpos, int inlength, double[] out, IntWrapper outpos) {
	// if (inpos.get() != 0) {
	// throw new UnsupportedOperationException("TODO");
	// } else if (outpos.get() != 0) {
	// throw new UnsupportedOperationException("TODO");
	// }
	//
	// }

	private long[] asArray(ByteBuffer buffer) {
		LongBuffer longBuffer = buffer.asLongBuffer();
		// if (longBuffer.hasArray()) {
		// return longBuffer.array();
		// }

		long[] array = new long[longBuffer.limit()];
		longBuffer.get(array);

		return array;
	}

	@Override
	public void uncompress(ByteBuffer buffer, double[] doubles) {
		buffer.position(0);
		// LongBuffer longBuffer = buffer.asLongBuffer();

		int[] compressedArrangement = new int[ARRANGEMENT_COMPRESSED_INTS];

		int size = doubles.length;
		long[] compressedLongs = asArray(buffer);
		int[] arrangement = new int[64];
		{
			for (int i = 0; i < compressedArrangement.length / 2; i++) {
				compressedArrangement[i * 2] = RoaringIntPacking.high(compressedLongs[i]);
				compressedArrangement[i * 2 + 1] = RoaringIntPacking.low(compressedLongs[i]);
			}

			IntWrapper outPositionArrangement = new IntWrapper();
			intCodec.uncompress(compressedArrangement,
					new IntWrapper(),
					compressedArrangement.length,
					arrangement,
					outPositionArrangement);
			assert outPositionArrangement.get() == arrangement.length;
		}

		long[] uncompressed = new long[size];
		{
			longCodec.uncompress(compressedLongs,
					new IntWrapper(compressedArrangement.length / 2),
					compressedLongs.length - compressedArrangement.length / 2,
					uncompressed,
					new IntWrapper());
		}
		for (int index = 0; index < doubles.length; index++) {
			long l = uncompressed[index];
			long reArranged = reverseArrange(arrangement, l);

			if (index >= 1) {
				reArranged ^= reverseArrange(arrangement, uncompressed[index - 1]);
			}

			doubles[index] = Double.longBitsToDouble(reArranged);
		}
	}

	// @Override
	// public void uncompress(double[] in, IntWrapper inpos, int inlength, double[] out, IntWrapper outpos) {
	// // TODO Auto-generated method stub
	//
	// }

}
