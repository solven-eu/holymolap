package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.AxesFilterAnd;
import eu.solven.holymolap.stable.v1.pojo.AxisEqualsFilter;

public class AggregateQueryBuilder {
	// By default, no columns
	protected final Set<String> wildcards = new ConcurrentSkipListSet<>();

	// By default, no filters
	protected final List<IAxesFilter> andFilters = new CopyOnWriteArrayList<>();

	// By default, no aggregation
	protected final List<IMeasuredAxis> measures = new CopyOnWriteArrayList<>();

	public AggregateQueryBuilder addWildcards(String wildcard, String... more) {
		return addWildcards(Lists.asList(wildcard, more));
	}

	public AggregateQueryBuilder addWildcards(Iterable<String> moreWildcards) {
		Iterables.addAll(this.wildcards, moreWildcards);

		return this;
	}

	public AggregateQueryBuilder andFilter(IAxesFilter moreAndFilter) {
		andFilters.add(moreAndFilter);

		return this;
	}

	public AggregateQueryBuilder addFilter(String key, Object value) {
		return andFilter(new AxisEqualsFilter(key, value));
	}

	public AggregateQueryBuilder addAggregations(Iterable<IMeasuredAxis> moreMeasures) {
		Iterables.addAll(this.measures, moreMeasures);

		return this;
	}

	public AggregateQueryBuilder addAggregations(IMeasuredAxis aggregatedAxis, IMeasuredAxis... more) {
		return addAggregations(Lists.asList(aggregatedAxis, more));
	}

	/**
	 * Like .addAggregation, but specialized for sums.
	 * 
	 * @param axis
	 *            the axis the aggregate with a SUM.
	 * @return current builder.
	 */
	public AggregateQueryBuilder sum(String axis) {
		return addAggregations(OperatorFactory.sum(axis));
	}

	/**
	 * Like .addAggregation, but specialized for counts.
	 * 
	 * @param axis
	 *            the axis the aggregate with a COUNT.
	 * @return current builder.
	 */
	public AggregateQueryBuilder count(String axis) {
		return addAggregations(OperatorFactory.count(axis));
	}

	public AggregateQueryBuilder cellCount(String axis) {
		return addAggregations(OperatorFactory.cellCount(axis));
	}

	public SimpleAggregationQuery build() {
		SimpleAggregationQuery query = new SimpleAggregationQuery(() -> new AxesFilterAnd(andFilters),
				() -> new ArrayList<>(wildcards),
				() -> measures);

		return query;
	}

	public static AggregateQueryBuilder grandTotal() {
		return new AggregateQueryBuilder();
	}

	public static AggregateQueryBuilder filter(String key, String value) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		queryBuilder.addFilter(key, value);

		return queryBuilder;
	}

	public static AggregateQueryBuilder wildcards(String wildcard, String... more) {
		return wildcards(Lists.asList(wildcard, more));
	}

	public static AggregateQueryBuilder wildcards(Iterable<String> wildcards) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		return queryBuilder.addWildcards(wildcards);
	}

	public static AggregateQueryBuilder edit(SimpleAggregationQuery base) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		for (IMeasuredAxis wildcard : base.getMeasures()) {
			queryBuilder.addAggregations(wildcard);
		}

		queryBuilder.andFilter(base.getFilters());

		for (String wildcard : base.getAxes()) {
			queryBuilder.addWildcards(wildcard);
		}

		return queryBuilder;
	}

}
