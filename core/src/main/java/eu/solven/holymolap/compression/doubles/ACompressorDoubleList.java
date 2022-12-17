package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

import eu.solven.holymolap.cache.CompressorDoubleArray;
import eu.solven.holymolap.cube.IMayCache;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which compress with {@link Compressor}.
 * 
 * @author Benoit Lacelle
 *
 */
// https://github.com/airlift/aircompressor
public class ACompressorDoubleList extends AbstractDoubleList implements IHasMemoryFootprint, IMayCache {
	final int size;

	final CompressorDoubleArray compressedArray;

	protected ACompressorDoubleList(ByteBuffer compressed,
			Compressor compressor,
			Decompressor decompressor,
			double[] array) {
		this.compressedArray = CompressorDoubleArray.compress(array, compressor, decompressor, compressed);
		this.size = array.length;
	}

	public ACompressorDoubleList(Compressor compressor, Decompressor decompressor, double[] array) {
		this.compressedArray = CompressorDoubleArray.compress(array, compressor, decompressor);
		this.size = array.length;
	}

	@Override
	public double getDouble(int index) {
		return compressedArray.getUncompressed()[index];
	}

	// Beware: the output array should not be written
	@Override
	public double[] toDoubleArray() {
		return compressedArray.getUncompressed();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public long getSizeInBytes() {
		return compressedArray.getSizeInBytes();
	}

	@Override
	public void invalidateCache() {
		compressedArray.invalidateCache();
	}
}
