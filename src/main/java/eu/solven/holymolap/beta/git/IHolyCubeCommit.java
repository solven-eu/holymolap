package eu.solven.holymolap.beta.git;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.stable.v1.IOneShotAggregator;

/**
 * An {@link IHolyCubeCommit} refers to an {@link IHolyCube} holding data, and a set of data to mask from previous
 * {@link IHolyCubeCommit}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCubeCommit extends IOneShotAggregator, IHasFilters {

	/**
	 * 
	 * @return the {@link IHolyCubeCommit} over which this has been appended.
	 */
	IHolyCubeCommit getParent();

	/**
	 * 
	 * @return the data held by current node.
	 */
	IHolyCube getCube();

	/**
	 * @return the mask, hiding data from previous nodes.
	 */
	@Override
	IAxesFilter getFilters();
}
