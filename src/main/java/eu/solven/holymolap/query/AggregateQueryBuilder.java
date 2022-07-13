package eu.solven.holymolap.query;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class AggregateQueryBuilder {
	protected final Set<String> wildcards = new ConcurrentSkipListSet<>();
	protected final Map<String, Object> filters = new ConcurrentSkipListMap<>();

	protected final Set<String> aggregatesKeys = new ConcurrentSkipListSet<>();

	public AggregateQueryBuilder addWildcard(String wildcard) {
		wildcards.add(wildcard);

		return this;
	}

	public AggregateQueryBuilder addFilter(String key, Object value) {
		filters.put(key, value);

		return this;
	}

	public AggregateQueryBuilder addAggregation(String key) {
		aggregatesKeys.add(key);

		return this;
	}

	public AggregateQuery build() {
		AggregateQuery query = new AggregateQuery(filters, wildcards, aggregatesKeys);

		return query;
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
