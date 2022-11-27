//package eu.solven.holymolap.stable.v1;
//
//import java.util.List;
//
///**
// * Describes {@link IHasFilters} and {@link IHasFilters} for {@link IAggregationQuery}, but not aggregations
// * 
// * @author blacelle
// *
// */
//public interface IFiltersAndColumnsForAggregationQuery extends IHasFilters, IHasAxes {
//
//	/**
//	 * The filters of current query. A filter refers to the condition for the data to be included. An empty {@link List}
//	 * means the whole data has to be included. Exclusions can be done through {@link IExclusionFilter}
//	 * 
//	 * @return a list of filters (to be interpreted as an OR over AND simple conditions).
//	 */
//	@Override
//	IAxesFilter getFilters();
//
//	/**
//	 * The columns amongst which the result has to be ventilated/sliced.
//	 * 
//	 * @return a Set of columns
//	 */
//	@Override
//	List<String> getAxes();
//
//	/**
//	 * 
//	 * @param hasAggregations
//	 * @return a new {@link IAggregationQuery} based on input {@link IHasAggregations}
//	 */
//	IAggregationQuery addAggregations(IHasAggregations hasAggregations);
//}
