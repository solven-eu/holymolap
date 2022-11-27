package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which split doubles between the exponentBytes and the mantissaBytes.
 * 
 * @author Benoit Lacelle
 *
 */
// https://en.wikipedia.org/wiki/Double-precision_floating-point_format
public class ADoubleCodecCompressedDoubleColumn extends AbstractDoubleList implements IHasMemoryFootprint {
	final int size;
	final transient IDoubleCodec doubleCodec;

	final ByteBuffer compressed;

	protected ADoubleCodecCompressedDoubleColumn(ByteBuffer compressed, IDoubleCodec codec, double[] array) {
		this.doubleCodec = codec;

		this.size = array.length;

		// https://www.baeldung.com/java-bytebuffer
		compressed.clear();
		codec.compress(array, compressed);
		compressed.flip();

		if (compressed.position() < compressed.capacity()) {
			ByteBuffer trimmedCompressed = ByteBuffer.allocate(compressed.limit());
			// compressed.mark();
			// compressed.position(0);
			trimmedCompressed.put(compressed);
			trimmedCompressed.flip();
			this.compressed = trimmedCompressed;
		} else {
			this.compressed = compressed;
		}
	}

	public ADoubleCodecCompressedDoubleColumn(IDoubleCodec codec, double[] array) {
		// Worst case: we consume twice more space than input double[], plus a header of 8 doubles
		// see eu.solven.holymolap.compression.doubles.ReOrderVariableByteDoubleCodec.ARRANGEMENT_COMPRESSED_INTS
		this(ByteBuffer.allocate(array.length * Double.BYTES * 2 + Double.BYTES * 8), codec, array);
	}

	@Override
	public double getDouble(int index) {
		double[] outputBuffer = new double[size];

		// TODO Implement SkippableDoubleCodec
		doubleCodec.uncompress(compressed, outputBuffer);

		return outputBuffer[index];
	}

	@Override
	public double[] toDoubleArray() {
		double[] outputBuffer = new double[size];

		doubleCodec.uncompress(compressed, outputBuffer);

		return outputBuffer;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public long getSizeInBytes() {
		return this.compressed.capacity() * Byte.BYTES;
	}
}
