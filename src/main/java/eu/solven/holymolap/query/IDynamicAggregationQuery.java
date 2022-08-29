package eu.solven.holymolap.query;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IHasAggregations;

/**
 * This extends {@link IAggregationQuery} by enabling easy creation of derivated queries.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IDynamicAggregationQuery extends IAggregationQuery {
	IDynamicAggregationQuery addAggregations(IHasAggregations aggregations);
}
