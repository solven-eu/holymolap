package eu.solven.holymolap.cube;

import eu.solven.holymolap.cube.cellset.ILazyHolyCellMultiSet;

/**
 * An {@link HolyCube} enabling {@link ILazyHolyCellMultiSet}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ILazyHolyCube extends IHolyCube {

	ILazyHolyCellMultiSet getCellSet();
}
