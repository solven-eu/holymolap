package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.query.SingleColumnAggregationLogic;
import eu.solven.holymolap.query.operator.IStandardOperators;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.ISinkContext;
import eu.solven.holymolap.sink.ObjectOnlySinkContext;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.stable.v1.IAggregationQuery;

public class TestAggregation {
	public static final String FIRST_KEY = "firstKey";
	public static final String FIRST_VALUE = "firstValue";

	public static final String SECOND_KEY = "secondKey";
	public static final String SECOND_VALUE = "secondValue";

	public static final String DOUBLE_FIRSY_KEY = "doubleFirstKey";
	public static final double DOUBLE_FIRST_VALUE = 13D;

	public static final String DOUBLE_SECOND_KEY = "doubleSecondKey";
	public static final double DOUBLE_SECOND_VALUE = 17D;

	public static IAggregationQuery GRAND_TOTAL = SimpleAggregationQuery.GRAND_TOTAL;

	public static SimpleAggregationQuery FILTER_FIRST_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build();
	public static SimpleAggregationQuery DD_FIRST_KEY = AggregateQueryBuilder.wildcard(FIRST_KEY).build();

	public static SimpleAggregationQuery FILTER_FIRST_FILTER_SECOND_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addFilter(SECOND_KEY, SECOND_VALUE).build();
	public static SimpleAggregationQuery DD_FIRST_DD_SECOND_KEY =
			AggregateQueryBuilder.wildcard(FIRST_KEY).addWildcard(SECOND_KEY).build();
	public static SimpleAggregationQuery FILTER_FIRST_DD_SECOND_KEY =
			AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addWildcard(SECOND_KEY).build();

	protected List<IAggregationQuery> getAllQueries() {
		List<IAggregationQuery> queries = new ArrayList<>();

		queries.add(GRAND_TOTAL);

		queries.add(FILTER_FIRST_KEY);
		queries.add(DD_FIRST_KEY);

		queries.add(FILTER_FIRST_FILTER_SECOND_KEY);
		queries.add(DD_FIRST_DD_SECOND_KEY);
		queries.add(FILTER_FIRST_DD_SECOND_KEY);

		return queries;
	}

	@Test
	public void testEmptyCube() {
		HolyCube cube = new HolyCube();

		Assert.assertEquals(0, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getIndex().keySet());

		for (IAggregationQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(),
					AggregateHelper.cumulateInNavigableMap(cube,
							query,
							new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IStandardOperators.SUM)));
		}
	}

	@Test
	public void testAddOneEmptyEntry() {
		ObjectOnlySinkContext context = new ObjectOnlySinkContext(new String[] {});
		IHolyCube cube = new HolyCubeSink().sink(context, new FastEntry(new Object[] {}));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getIndex().keySet());

		for (IAggregationQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(),
					AggregateHelper.cumulateInNavigableMap(cube,
							query,
							new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IStandardOperators.SUM)));
		}
	}

	@Test
	public void testAddOneEntryAggregateNotDoubleKey() {
		IHolyCube cube = new HolyCubeSink().sink(new ObjectOnlySinkContext(new String[] { FIRST_KEY }),
				new FastEntry(new Object[] { FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY)), cube.getIndex().keySet());

		Assert.assertEquals(new TreeMap<>(),
				AggregateHelper.cumulateInNavigableMap(cube,
						SimpleAggregationQuery.GRAND_TOTAL,
						new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IStandardOperators.SUM)));

		// There is no double on FirstKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap =
					AggregateHelper.cumulateInNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).sum(FIRST_KEY).build(),
							new SingleColumnAggregationLogic(FIRST_KEY, IStandardOperators.SUM));
			Assertions.assertThat(resultAsMap).hasSize(1);

			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(IStandardOperators.SUM.neutral(), resultAsMap.get(new TreeMap<>()), 0.001D);
		}
	}

	@Test
	public void testAddOneEntryAggregateDoubleKey() {
		ISinkContext context = new ObjectOnlySinkContext(new String[] { FIRST_KEY, DOUBLE_FIRSY_KEY });
		IHolyCube cube =
				new HolyCubeSink().sink(context, new FastEntry(new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY, DOUBLE_FIRSY_KEY)), cube.getIndex().keySet());

		Assert.assertEquals(new TreeMap<>(),
				AggregateHelper.cumulateInNavigableMap(cube,
						SimpleAggregationQuery.GRAND_TOTAL,
						new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IStandardOperators.SUM)));

		// THere is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap =
					AggregateHelper.cumulateInNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).sum(DOUBLE_FIRSY_KEY).build(),
							new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IStandardOperators.SUM));
			Assertions.assertThat(resultAsMap).hasSize(1);

			// https://github.com/joel-costigliola/assertj-core/issues/315
			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(DOUBLE_FIRST_VALUE, resultAsMap.get(new TreeMap<>()).doubleValue(), 0.001D);
		}
	}
}
