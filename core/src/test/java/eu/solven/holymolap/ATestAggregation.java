package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.assertj.core.api.Assertions;

import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.stable.v1.IAggregationQuery;

public class ATestAggregation implements IHolyMapDataTestConstants {

	public static IAggregationQuery GRAND_TOTAL = AggregateQueryBuilder.grandTotal().build();

	public static IAggregationQuery GRAND_TOTAL_COUNT =
			AggregateQueryBuilder.grandTotal().count(ICountMeasuresConstants.STAR).build();
	public static IAggregationQuery GRAND_TOTAL_CELLCOUNT =
			AggregateQueryBuilder.grandTotal().cellCount(ICountMeasuresConstants.STAR).build();

	public static SimpleAggregationQuery FILTER_FIRST_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build();
	public static SimpleAggregationQuery DD_FIRST_KEY = AggregateQueryBuilder.wildcards(FIRST_KEY).build();

	public static SimpleAggregationQuery FILTER_FIRST_FILTER_SECOND_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addFilter(SECOND_KEY, SECOND_VALUE).build();
	public static SimpleAggregationQuery DD_FIRST_DD_SECOND_KEY =
			AggregateQueryBuilder.wildcards(FIRST_KEY).addWildcards(SECOND_KEY).build();
	public static SimpleAggregationQuery FILTER_FIRST_DD_SECOND_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addWildcards(SECOND_KEY).build();

	public static List<IAggregationQuery> getAllQueries() {
		List<IAggregationQuery> queries = new ArrayList<>();

		queries.add(GRAND_TOTAL);

		queries.add(FILTER_FIRST_KEY);
		queries.add(DD_FIRST_KEY);

		queries.add(FILTER_FIRST_FILTER_SECOND_KEY);
		queries.add(DD_FIRST_DD_SECOND_KEY);
		queries.add(FILTER_FIRST_DD_SECOND_KEY);

		return queries;
	}

	public static void assertEmptyOrNeutral(NavigableMap<? extends NavigableMap<?, ?>, ?> result, Object neutral) {
		if (result.isEmpty()) {
			// The measures has no contributing rows: OK
		} else {
			// Everything equals the neutral
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(neutral);
			});
		}
	}

}
