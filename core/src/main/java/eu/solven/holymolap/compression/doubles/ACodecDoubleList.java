package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

import eu.solven.holymolap.cache.CompressedDoubleArray;
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
public class ACodecDoubleList extends AbstractDoubleList implements IHasMemoryFootprint {
	final int size;

	final CompressedDoubleArray compressedArray;

	protected ACodecDoubleList(ByteBuffer compressed, IDoubleCodec codec, double[] array) {
		this.compressedArray = CompressedDoubleArray.compress(array, codec, compressed);
		this.size = array.length;
	}

	public ACodecDoubleList(IDoubleCodec codec, double[] array) {
		this.compressedArray = CompressedDoubleArray.compress(array, codec);
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
}
