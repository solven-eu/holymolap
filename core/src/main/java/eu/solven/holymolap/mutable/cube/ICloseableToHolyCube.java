package eu.solven.holymolap.mutable.cube;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * Any mutable resource which can be closed into a {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public interface ICloseableToHolyCube {

	/**
	 * Will flush for pending operations, and return an immutable {@link IHolyCube}
	 * 
	 * @return {@link IHolyCube} holding the data of current {@link ICloseableToHolyCube}
	 */
	IHolyCube closeToHolyCube();
}
