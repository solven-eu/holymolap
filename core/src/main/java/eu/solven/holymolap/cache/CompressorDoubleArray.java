package eu.solven.holymolap.cache;

import java.nio.ByteBuffer;

import com.google.common.primitives.Doubles;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;

public class CompressorDoubleArray extends AHasSoftRefCache<double[], ByteBuffer> {

	final int sizeUncompressed;
	protected final Compressor compressor;
	protected final Decompressor decompressor;

	public CompressorDoubleArray(ByteBuffer compressed,
			Compressor compressor,
			Decompressor decompressor,
			double[] uncompressed) {
		super(uncompressed, compressed);

		this.sizeUncompressed = uncompressed.length;
		this.compressor = compressor;
		this.decompressor = decompressor;
	}

	public static CompressorDoubleArray compress(double[] uncompressed,
			Compressor compressor,
			Decompressor decompressor,
			ByteBuffer compressedBuffer) {
		ByteBuffer uncompressedAsBB = ByteBuffer.allocate(uncompressed.length * Doubles.BYTES);
		uncompressedAsBB.asDoubleBuffer().put(uncompressed);

		// https://www.baeldung.com/java-bytebuffer
		compressedBuffer.clear();
		compressor.compress(uncompressedAsBB, compressedBuffer);
		compressedBuffer.flip();

		if (compressedBuffer.position() < compressedBuffer.capacity()) {
			ByteBuffer trimmedCompressed = ByteBuffer.allocate(compressedBuffer.limit());
			trimmedCompressed.put(compressedBuffer);
			trimmedCompressed.flip();
			compressedBuffer = trimmedCompressed;
		}

		return new CompressorDoubleArray(compressedBuffer, compressor, decompressor, uncompressed);
	}

	public static CompressorDoubleArray compress(double[] uncompressed,
			Compressor compressor,
			Decompressor decompressor) {
		return compress(uncompressed,
				compressor,
				decompressor,
				ByteBuffer.allocate(compressor.maxCompressedLength(Double.BYTES * uncompressed.length)));
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
		ByteBuffer uncompressedAsBB = ByteBuffer.allocate(sizeUncompressed * Doubles.BYTES);

		decompressor.decompress(compressed, uncompressedAsBB);

		double[] array = new double[sizeUncompressed];
		uncompressedAsBB.asDoubleBuffer().get(array);

		return array;
	}

}
