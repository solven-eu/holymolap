package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.AxesFilterAnd;

public class AggregateQueryBuilder {
	// By default, no columns
	protected final Set<String> wildcards = new ConcurrentSkipListSet<>();

	// By default, no filters
	protected final Map<String, Object> filters = new ConcurrentSkipListMap<>();

	// By default, no aggregation
	protected final List<IMeasuredAxis> aggregatesKeys = new CopyOnWriteArrayList<>();

	public AggregateQueryBuilder addWildcard(String wildcard) {
		wildcards.add(wildcard);

		return this;
	}

	public AggregateQueryBuilder addFilter(String key, Object value) {
		filters.put(key, value);

		return this;
	}

	public AggregateQueryBuilder addAggregation(IMeasuredAxis aggregatedAxis) {
		aggregatesKeys.add(aggregatedAxis);

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
		SimpleAggregationQuery query = new SimpleAggregationQuery(() -> new AxesFilterAnd(filters),
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

}
