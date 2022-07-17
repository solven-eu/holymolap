package eu.solven.holymolap.stable.v1;

import java.util.List;

/**
 * A aggregation query. It is configured by:
 * 
 * - a filtering condition - axes along which the result is sliced - aggregations accumulating some measures
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregationQuery extends IHasFilters, IHasColumns {
	/**
	 * The filters of current query. A filter refers to the condition for the data to be included. An empty {@link List}
	 * means the whole data has to be included. Exclusions can be done through {@link IExclusionFilter}
	 * 
	 * @return a list of filters (to be interpreted as an OR over AND simple conditions).
	 */
	@Override
	IAxesFilter getFilters();

	/**
	 * The columns amongst which the result has to be ventilated/sliced.
	 * 
	 * @return a Set of columns
	 */
	@Override
	List<String> getColumns();

	List<IAggregatedAxis> getAggregations();

}
