package eu.solven.holymolap.compression.doubles;

import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} based on {@link GZIPInputStream}
 * 
 * @author Benoit Lacelle
 *
 */
public class GZipDoubleList extends ACodecDoubleList {

	public GZipDoubleList(double[] array) {
		super(new GZipDoubleCodec(), array);
	}
}
