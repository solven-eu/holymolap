package eu.solven.holymolap.git;

import eu.solven.holymolap.stable.v1.IOneShotAggregator;

/**
 * An {@link IHolyCubeBranch} is a chain of {@link IHolyCubeCommit}, enabling aggregation queries. Its head may
 * change, while new node are appended into the chain. One can append an empty node, masking some data to simulate
 * data-removal.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCubeBranch extends IOneShotAggregator {
	IHolyCubeCommit getHead();
}
