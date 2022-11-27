package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

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
	protected final List<IMeasuredAxis> aggregatesKeys = new CopyOnWriteArrayList<>();

	public AggregateQueryBuilder addWildcard(String wildcard) {
		wildcards.add(wildcard);

		return this;
	}

	public AggregateQueryBuilder andFilter(IAxesFilter filter) {
		andFilters.add(filter);

		return this;
	}

	public AggregateQueryBuilder addFilter(String key, Object value) {
		return andFilter(new AxisEqualsFilter(key, value));
	}

	public AggregateQueryBuilder addAggregation(IMeasuredAxis aggregatedAxis, IMeasuredAxis... more) {
		aggregatesKeys.add(aggregatedAxis);
		aggregatesKeys.addAll(Arrays.asList(more));

		return this;
	}

	/**
	 * Like .addAggregation, but specialized for sums.
	 * 
	 * @param axis
	 *            the axis the aggregate with a SUM.
	 * @return current builder.
	 */
	public AggregateQueryBuilder sum(String axis) {
		return addAggregation(OperatorFactory.sum(axis));
	}

	/**
	 * Like .addAggregation, but specialized for counts.
	 * 
	 * @param axis
	 *            the axis the aggregate with a COUNT.
	 * @return current builder.
	 */
	public AggregateQueryBuilder count(String axis) {
		return addAggregation(OperatorFactory.count(axis));
	}

	public AggregateQueryBuilder cellCount(String axis) {
		return addAggregation(OperatorFactory.cellCount(axis));
	}

	public SimpleAggregationQuery build() {
		SimpleAggregationQuery query = new SimpleAggregationQuery(() -> new AxesFilterAnd(andFilters),
				() -> new ArrayList<>(wildcards),
				() -> aggregatesKeys);

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

	public static AggregateQueryBuilder wildcard(String wildcard) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		queryBuilder.addWildcard(wildcard);

		return queryBuilder;
	}

	public static AggregateQueryBuilder wildcards(Iterable<String> wildcards) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		for (String wildcard : wildcards) {
			queryBuilder.addWildcard(wildcard);
		}

		return queryBuilder;
	}

	public static AggregateQueryBuilder edit(SimpleAggregationQuery base) {
		AggregateQueryBuilder queryBuilder = new AggregateQueryBuilder();

		for (IMeasuredAxis wildcard : base.getMeasures()) {
			queryBuilder.addAggregation(wildcard);
		}

		queryBuilder.andFilter(base.getFilters());

		for (String wildcard : base.getAxes()) {
			queryBuilder.addWildcard(wildcard);
		}

		return queryBuilder;
	}

}