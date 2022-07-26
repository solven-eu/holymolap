package eu.solven.holymolap.cube;

import eu.solven.holymolap.cube.index.ILazyHolyCubeIndex;

/**
 * An {@link HolyCube} enabling {@link ILazyHolyCubeIndex}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ILazyHolyCube extends IHolyCube {

	ILazyHolyCubeIndex getIndex();
}
