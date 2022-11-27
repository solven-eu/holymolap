package eu.solven.holymolap.cube.immutable.column;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.mutable.cellset.FibonacciEncoding;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.longcompression.LongVariableByte;
import me.lemire.longcompression.RoaringIntPacking;

/**
 * A {@link DoubleList} which split doubles between the exponentBytes and the mantissaBytes.
 * 
 * @author Benoit Lacelle
 *
 */
public class CompressedDoubleColumn_v2 extends AbstractDoubleList {
	private static final Logger LOGGER = LoggerFactory.getLogger(CompressedDoubleColumn_v2.class);

	private static final int ARRANGEMENT_COMPRESSED_INTS = 16;

	final int size;

	final long[] compressedLongs;

	public CompressedDoubleColumn_v2(double[] array) {
		this.size = array.length;

		int[] nbChanges = new int[64];

		long[] asLongs = DoubleStream.of(array).mapToLong(d -> Double.doubleToRawLongBits(d)).toArray();

		for (int i = 1; i < asLongs.length; i++) {
			for (int bit = 0; bit < 64; bit++) {
				if ((asLongs[i - 1] & (Long.MIN_VALUE >>> bit)) != (asLongs[i] & (Long.MIN_VALUE >>> bit))) {
					nbChanges[bit]++;
				}
			}
		}

		int[] lessFrequentFirst = IntStream.range(0, 64)
				.mapToObj(i -> i)
				.sorted(Comparator.comparing(i -> nbChanges[i]))
				.mapToInt(i -> i)
				.toArray();

		// With variableByte, the compressed output of the N-th first integers (whatever the order) is always 16
		int[] compressedArrangement = new int[ARRANGEMENT_COMPRESSED_INTS];
		IntWrapper outPositionArrangement = new IntWrapper();
		new VariableByte()
				.compress(lessFrequentFirst, new IntWrapper(), 64, compressedArrangement, outPositionArrangement);
		assert outPositionArrangement.get() == compressedArrangement.length;

		long[] compressedArrangementAsLongs = new long[compressedArrangement.length / 2];
		for (int i = 0; i < compressedArrangementAsLongs.length; i++) {
			compressedArrangementAsLongs[i] =
					RoaringIntPacking.pack(compressedArrangement[i * 2], compressedArrangement[i * 2 + 1]);
		}

		// https://www.timescale.com/blog/time-series-compression-algorithms-explained/
		// https://www.baeldung.com/java-xor-operator
		// https://github.com/burmanm/gorilla-tsc
		long[] asXorLongs = new long[asLongs.length];
		asXorLongs[0] = asLongs[0];
		for (int i = 1; i < asLongs.length; i++) {
			asXorLongs[i] = asLongs[i] ^ asLongs[i - 1];
		}

		// We expect the arranged longs to be generally small (to push 0 bits on the left)
		long[] asArrangedLongs =
				LongStream
						.concat(LongStream.of(compressedArrangementAsLongs),
								LongStream.of(asXorLongs).map(l -> reArrange(lessFrequentFirst, l)))
						.toArray();

		LongStream.of(asArrangedLongs).forEach(l -> System.out.println(FibonacciEncoding.longToBinaryWithLeading(l)));

		IntWrapper outPosition = new IntWrapper();
		long[] compressed = new long[asArrangedLongs.length * 2];
		new LongVariableByte()
				.compress(asArrangedLongs, new IntWrapper(), asArrangedLongs.length, compressed, outPosition);

		compressedLongs = Arrays.copyOf(compressed, outPosition.get());
	}

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

	@Override
	public double getDouble(int index) {
		int[] compressedArrangement = new int[ARRANGEMENT_COMPRESSED_INTS];

		long[] uncompressed = new long[compressedArrangement.length / 2 + size];
		IntWrapper outPosition = new IntWrapper();
		new LongVariableByte()
				.uncompress(compressedLongs, new IntWrapper(), compressedLongs.length, uncompressed, outPosition);
		assert outPosition.get() == uncompressed.length;

		for (int i = 0; i < compressedArrangement.length / 2; i++) {
			compressedArrangement[i * 2] = RoaringIntPacking.high(uncompressed[i]);
			compressedArrangement[i * 2 + 1] = RoaringIntPacking.low(uncompressed[i]);
		}

		int[] arrangement = new int[64];
		IntWrapper outPositionArrangement = new IntWrapper();
		new VariableByte().uncompress(compressedArrangement,
				new IntWrapper(),
				compressedArrangement.length,
				arrangement,
				outPositionArrangement);
		assert outPositionArrangement.get() == arrangement.length;

		long l = uncompressed[8 + index];
		long reArranged = reverseArrange(arrangement, l);

		if (index >= 1) {
			reArranged ^= reverseArrange(arrangement, uncompressed[8 + index - 1]);
		}

		return Double.longBitsToDouble(reArranged);
	}

	@Override
	public int size() {
		return size;
	}
}