package eu.solven.holymolap.compression.doubles;

import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which split doubles between the exponentBytes and the mantissaBytes.
 * 
 * @author Benoit Lacelle
 *
 */
public class GZipDoubleList extends ACodecDoubleList {

	public GZipDoubleList(double[] array) {
		super(new GZipDoubleCodec(), array);
	}
}
