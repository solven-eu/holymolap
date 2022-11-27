//package eu.solven.holymolap.stable.v1.pojo;
//
//import java.util.Collections;
//import java.util.List;
//
//import eu.solven.holymolap.query.SimpleAggregationQuery;
//import eu.solven.holymolap.stable.v1.IAggregationQuery;
//import eu.solven.holymolap.stable.v1.IAxesFilter;
//import eu.solven.holymolap.stable.v1.IFiltersAndColumnsForAggregationQuery;
//import eu.solven.holymolap.stable.v1.IHasAggregations;
//import eu.solven.holymolap.stable.v1.IHasFilters;
//
///**
// * Describes {@link IHasFilters} and {@link IHasFilters} for {@link IAggregationQuery}, but not aggregations
// * 
// * @author Benoit Lacelle
// *
// */
//public class FiltersAndColumnsForAggregationQuery implements IFiltersAndColumnsForAggregationQuery {
//
//	public static final FiltersAndColumnsForAggregationQuery GRAND_TOTAL =
//			new FiltersAndColumnsForAggregationQuery(new AxesFilterAnd(Collections.emptyMap()),
//					Collections.emptyList());
//
//	final IAxesFilter filters;
//	final List<String> axes;
//
//	public FiltersAndColumnsForAggregationQuery(IAxesFilter filters, List<String> columns) {
//		this.filters = filters;
//		this.axes = columns;
//	}
//
//	@Override
//	public IAxesFilter getFilters() {
//		return filters;
//	}
//
//	@Override
//	public List<String> getAxes() {
//		return axes;
//	}
//
//	// @Override
//	public IAggregationQuery addAggregations(IHasAggregations hasAggregations) {
//		return new SimpleAggregationQuery(this, this, hasAggregations);
//	}
//}
