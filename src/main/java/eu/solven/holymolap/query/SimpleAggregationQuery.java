package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import eu.solven.holymolap.stable.v1.IAggregatedAxis;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.pojo.AndAxesFilter;

/**
 * Simple {@link IAggregationQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class SimpleAggregationQuery implements IAggregationQuery {

	public static final IAggregationQuery GRAND_TOTAL =
			new SimpleAggregationQuery(Collections.emptyMap(), Collections.emptySet(), Collections.emptyList());

	protected final Map<String, Object> filters = new ConcurrentSkipListMap<>();
	protected final List<String> wildcards = new ArrayList<>();

	protected final List<IAggregatedAxis> aggregatedKeys = new ArrayList<>();

	public SimpleAggregationQuery(Map<String, ?> filters, Set<String> wildcards, List<IAggregatedAxis> aggregatedKeys) {
		this.filters.putAll(filters);
		this.wildcards.addAll(wildcards);
		this.aggregatedKeys.addAll(aggregatedKeys);
	}

	@Override
	public IAxesFilter getFilters() {
		return new AndAxesFilter(filters);
	}

	@Override
	public List<String> getColumns() {
		return Collections.unmodifiableList(wildcards);
	}

	@Override
	public List<IAggregatedAxis> getAggregations() {
		return Collections.unmodifiableList(aggregatedKeys);
	}
}
