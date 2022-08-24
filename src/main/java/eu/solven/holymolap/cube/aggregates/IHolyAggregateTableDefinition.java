package eu.solven.holymolap.cube.aggregates;

import java.util.List;

/**
 * Describes a set of aggregation logics
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyAggregateTableDefinition {
	/**
	 * 
	 * @return the (distinct) ordered aggregations
	 */
	List<IHolyAggregatedColumnMeta> aggregations();

	int getAggregationIndex(IHolyAggregatedColumnMeta aggregation);
}
