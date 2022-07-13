package eu.solven.holymolap.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class AggregateQuery implements IAggregateQuery {
	public static final AggregateQuery GRAND_TOTAL =
			new AggregateQuery(Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

	protected final Map<String, Object> filters = new ConcurrentSkipListMap<>();
	protected final Set<String> wildcards = new ConcurrentSkipListSet<>();

	protected final Set<Object> aggregatedKeys = new ConcurrentSkipListSet<>();

	public AggregateQuery(Map<String, ?> filters, Set<String> wildcards, Set<?> aggregatedKeys) {
		this.filters.putAll(filters);
		this.wildcards.addAll(wildcards);
		this.aggregatedKeys.addAll(aggregatedKeys);
	}

	@Override
	public List<? extends Map<String, ?>> getFilters() {
		return Arrays.asList(filters);
	}

	@Override
	public Set<String> getWildcards() {
		return wildcards;
	}
}
