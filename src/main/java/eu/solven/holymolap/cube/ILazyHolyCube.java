package eu.solven.holymolap.cube;

import eu.solven.holymolap.cube.index.ILazyHolyCellSet;

/**
 * An {@link HolyCube} enabling {@link ILazyHolyCellSet}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ILazyHolyCube extends IHolyCube {

	ILazyHolyCellSet getCellSet();
}
