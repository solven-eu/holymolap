package eu.solven.holymolap.cache;

import java.nio.ByteBuffer;

import eu.solven.holymolap.compression.doubles.IDoubleCodec;

public class CompressedDoubleArray extends AHasSoftRefCache<double[], ByteBuffer> {

	final int sizeUncompressed;
	protected final IDoubleCodec doubleCodec;

	public CompressedDoubleArray(ByteBuffer compressed, IDoubleCodec codec, double[] uncompressed) {
		super(uncompressed, compressed);

		this.sizeUncompressed = uncompressed.length;
		this.doubleCodec = codec;
	}

	public static CompressedDoubleArray compress(double[] uncompressed,
			IDoubleCodec codec,
			ByteBuffer compressedBuffer) {
		// https://www.baeldung.com/java-bytebuffer
		compressedBuffer.clear();
		codec.compress(uncompressed, compressedBuffer);
		compressedBuffer.flip();

		if (compressedBuffer.position() < compressedBuffer.capacity()) {
			ByteBuffer trimmedCompressed = ByteBuffer.allocate(compressedBuffer.limit());
			trimmedCompressed.put(compressedBuffer);
			trimmedCompressed.flip();
			compressedBuffer = trimmedCompressed;
		}

		return new CompressedDoubleArray(compressedBuffer, codec, uncompressed);
	}

	public static CompressedDoubleArray compress(double[] uncompressed, IDoubleCodec codec) {
		// Worst case: we consume twice more space than input double[], plus a header of 8 doubles
		// see eu.solven.holymolap.compression.doubles.ReOrderVariableByteDoubleCodec.ARRANGEMENT_COMPRESSED_INTS
		return compress(uncompressed,
				codec,
				ByteBuffer.allocate(uncompressed.length * Double.BYTES * 2 + Double.BYTES * 8));
	}

	@Override
	protected long getSizeInBytesCompressed(ByteBuffer structure) {
		return structure.capacity() * Byte.BYTES;
	}

	@Override
	protected long getSizeInBytesUncompressed(double[] structure) {
		return structure.length * Double.BYTES;
	}

	@Override
	protected double[] uncompress() {
		double[] outputBuffer = new double[sizeUncompressed];

		// TODO Implement SkippableDoubleCodec
		doubleCodec.uncompress(compressed, outputBuffer);

		return outputBuffer;
	}

}
