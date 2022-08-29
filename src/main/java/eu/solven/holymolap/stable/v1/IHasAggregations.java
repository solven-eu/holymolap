package eu.solven.holymolap.stable.v1;

import java.util.List;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * A {@link List} of {@link IAggregatedAxis}. Typically used by {@link IAggregationQuery}, or {@link IHolyCube}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasAggregations {

	List<IAggregatedAxis> getAggregations();
}
