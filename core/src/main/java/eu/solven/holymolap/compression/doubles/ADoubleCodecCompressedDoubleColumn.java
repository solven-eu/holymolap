package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which split doubles between the exponentBytes and the mantissaBytes.
 * 
 * @author Benoit Lacelle
 *
 */
// https://en.wikipedia.org/wiki/Double-precision_floating-point_format
public class ADoubleCodecCompressedDoubleColumn extends AbstractDoubleList {
	final int size;
	final transient IDoubleCodec doubleCodec;

	final ByteBuffer compressed;

	public ADoubleCodecCompressedDoubleColumn(IDoubleCodec doubleCodec, double[] array) {
		this.doubleCodec = doubleCodec;

		this.size = array.length;

		this.compressed = ByteBuffer.allocate(array.length * 8 * 2);
		doubleCodec.compress(array, compressed);
	}

	@Override
	public double getDouble(int index) {
		double[] outputBuffer = new double[1];

		doubleCodec.uncompress(compressed, outputBuffer);

		return outputBuffer[0];
	}

	@Override
	public int size() {
		return size;
	}
}
