package eu.solven.holymolap.bitmap;

import java.util.function.LongConsumer;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * An interface of bitmap specialized for {@link IHolyCube}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyBitmap {
	/**
	 * 
	 * @return the number of different longs in current {@link IHolyBitmap}
	 */
	long getCardinality();

	long getMin();

	long getMax();

	void acceptLongConsumer(LongConsumer longConsumer);
}
