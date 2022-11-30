package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.mutable.cellset.FibonacciEncoding;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import me.lemire.integercompression.IntCompressor;

/**
 * A {@link DoubleList} which split doubles between the exponentBytes and the mantissaBytes.
 * 
 * @author Benoit Lacelle
 *
 */
// https://en.wikipedia.org/wiki/Double-precision_floating-point_format
public class DoubleAsFourBytesDoubleList extends AbstractDoubleList {
	private static final Logger LOGGER = LoggerFactory.getLogger(DoubleAsFourBytesDoubleList.class);

	private static final int CHUNKS_PER_LONG = 4;
	private static final int BYTES_PER_LONG = 8;
	private static final int BYTES_PER_CHUNCK = BYTES_PER_LONG / CHUNKS_PER_LONG;

	final int size;
	// The first buffer will hold the signBit, and early bits of the exponent
	// The last buffer will hold the most precise bits of the floating
	// final List<ByteBuffer> byteBuffers;

	final transient IntCompressor intCompressor = new IntCompressor();
	final List<int[]> compressedIntegers;

	public DoubleAsFourBytesDoubleList(double[] array) {
		this.size = array.length;

		final List<ByteBuffer> byteBuffers;
		byteBuffers = IntStream.range(0, CHUNKS_PER_LONG)
				.mapToObj(i -> ByteBuffer.allocate(size * BYTES_PER_CHUNCK))
				.collect(Collectors.toList());

		ByteBuffer longBytesReader = ByteBuffer.allocate(BYTES_PER_LONG);
		LongBuffer reader = longBytesReader.asLongBuffer();

		IntStream.range(0, array.length).forEach(index -> {
			double d = array[index];
			long bits = Double.doubleToRawLongBits(d);
			// assert FibonacciEncoding.longToBinaryWithLeading(bits)
			// .length() == 64 : "This is useful for debugging sessions";
			LOGGER.info("{} -> {}", d, FibonacciEncoding.longToBinaryWithLeading(bits));

			reader.position(0);
			reader.put(bits);

			longBytesReader.position(0);
			for (int chunkIndex = 0; chunkIndex < CHUNKS_PER_LONG; chunkIndex++) {
				longBytesReader.limit((1 + chunkIndex) * BYTES_PER_CHUNCK);
				byteBuffers.get(chunkIndex).put(longBytesReader);
			}
		});

		compressedIntegers = byteBuffers.stream().map(bb -> {
			bb.position(0);
			int[] intArray = new int[bb.capacity() / BYTES_PER_CHUNCK];

			if (BYTES_PER_CHUNCK != 2) {
				throw new UnsupportedOperationException(".getChar will not be valid");
			}

			for (int i = 0; i < intArray.length; i++) {
				intArray[i] = bb.getChar();
			}

			LOGGER.info("ints: {}", Arrays.toString(intArray));

			return intCompressor.compress(intArray);
		}).collect(Collectors.toList());
	}

	@Override
	public double getDouble(int index) {
		final List<ByteBuffer> byteBuffers;
		byteBuffers = compressedIntegers.stream()
				.map(compressedInts -> intCompressor.uncompress(compressedInts))
				.map(ints -> {
					ByteBuffer byteBuffer = ByteBuffer.allocate(4 * ints.length);
					CharBuffer intBuffer = byteBuffer.asCharBuffer();

					for (int charAsInt : ints) {
						intBuffer.put((char) charAsInt);
					}

					return byteBuffer;
				})
				.collect(Collectors.toList());

		ByteBuffer longBytesReader = ByteBuffer.allocate(8);

		for (int chunkIndex = 0; chunkIndex < CHUNKS_PER_LONG; chunkIndex++) {
			ByteBuffer byteBuffer = byteBuffers.get(chunkIndex);
			byteBuffer.position(index * BYTES_PER_CHUNCK);
			byteBuffer.limit((index + 1) * BYTES_PER_CHUNCK);
			longBytesReader.put(byteBuffer);
		}

		longBytesReader.position(0);
		LongBuffer reader = longBytesReader.asLongBuffer();
		long bits = reader.get();

		assert FibonacciEncoding.longToBinaryWithLeading(bits).length() == 64 : "This is useful for debugging sessions";

		return Double.longBitsToDouble(bits);
	}

	@Override
	public int size() {
		return size;
	}
}
