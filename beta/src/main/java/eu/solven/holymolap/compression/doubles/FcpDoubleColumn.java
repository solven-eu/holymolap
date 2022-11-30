package eu.solven.holymolap.compression.doubles;

import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which relies on FCP double compression
 * 
 * @author Benoit Lacelle
 *
 */
public class FcpDoubleColumn extends ACodecDoubleList {

	public FcpDoubleColumn(double[] array) {
		super(new FcpDoubleCodec(), array);
	}
}
