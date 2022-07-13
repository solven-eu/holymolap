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

import eu.solven.holymolap.IHolyCube;
import eu.solven.holymolap.RoaringCube;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQuery;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SingleColumnAggregationLogic;
import eu.solven.holymolap.query.operator.IDoubleBinaryOperator;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.ObjectOnlySinkContext;
import eu.solven.holymolap.sink.RoaringSink;

public class TestAggregation {
	public static final String FIRST_KEY = "firstKey";
	public static final String FIRST_VALUE = "firstValue";

	public static final String SECOND_KEY = "secondKey";
	public static final String SECOND_VALUE = "secondValue";

	public static final String DOUBLE_FIRSY_KEY = "doubleFirstKey";
	public static final double DOUBLE_FIRST_VALUE = 13D;

	public static final String DOUBLE_SECOND_KEY = "doubleSecondKey";
	public static final double DOUBLE_SECOND_VALUE = 17D;

	public static AggregateQuery GRAND_TOTAL = AggregateQuery.GRAND_TOTAL;

	public static AggregateQuery FILTER_FIRST_KEY = AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build();
	public static AggregateQuery DD_FIRST_KEY = AggregateQueryBuilder.wildcard(FIRST_KEY).build();

	public static AggregateQuery FILTER_FIRST_FILTER_SECOND_KEY = AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
			.addFilter(SECOND_KEY, SECOND_VALUE).build();
	public static AggregateQuery DD_FIRST_DD_SECOND_KEY = AggregateQueryBuilder.wildcard(FIRST_KEY).addWildcard(SECOND_KEY).build();
	public static AggregateQuery FILTER_FIRST_DD_SECOND_KEY = AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addWildcard(SECOND_KEY).build();

	protected List<AggregateQuery> getAllQueries() {
		List<AggregateQuery> queries = new ArrayList<>();

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
		RoaringCube cube = new RoaringCube();

		Assert.assertEquals(0, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getIndex().keySet());

		for (AggregateQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(), AggregateHelper.cumulateInNavigableMap(cube, query, new SingleColumnAggregationLogic(
					DOUBLE_FIRSY_KEY, IDoubleBinaryOperator.SUM)));
		}
	}

	@Test
	public void testAddOneEmptyEntry() {
		IHolyCube cube = new RoaringSink().sink(new FastEntry(new Object[] {}), new ObjectOnlySinkContext(new Object[] {}));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getIndex().keySet());

		for (AggregateQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(), AggregateHelper.cumulateInNavigableMap(cube, query, new SingleColumnAggregationLogic(
					DOUBLE_FIRSY_KEY, IDoubleBinaryOperator.SUM)));
		}
	}

	@Test
	public void testAddOneEntryAggregateNotDoubleKey() {
		IHolyCube cube = new RoaringSink()
				.sink(new FastEntry(new Object[] { FIRST_VALUE }), new ObjectOnlySinkContext(new Object[] { FIRST_KEY }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY)), cube.getIndex().keySet());

		Assert.assertEquals(new TreeMap<>(), AggregateHelper.cumulateInNavigableMap(cube, AggregateQuery.GRAND_TOTAL,
				new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IDoubleBinaryOperator.SUM)));

		// There is no double on FirstKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap = AggregateHelper.cumulateInNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addAggregation(FIRST_KEY).build(), new SingleColumnAggregationLogic(
							FIRST_KEY, IDoubleBinaryOperator.SUM));
			Assertions.assertThat(resultAsMap).hasSize(1);

			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(IDoubleBinaryOperator.SUM.neutral(), resultAsMap.get(new TreeMap<>()), 0.001D);
		}
	}

	@Test
	public void testAddOneEntryAggregateDoubleKey() {
		IHolyCube cube = new RoaringSink().sink(new FastEntry(new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE }), new ObjectOnlySinkContext(
				new Object[] { FIRST_KEY, DOUBLE_FIRSY_KEY }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY, DOUBLE_FIRSY_KEY)), cube.getIndex().keySet());

		Assert.assertEquals(new TreeMap<>(), AggregateHelper.cumulateInNavigableMap(cube, AggregateQuery.GRAND_TOTAL,
				new SingleColumnAggregationLogic(DOUBLE_FIRSY_KEY, IDoubleBinaryOperator.SUM)));

		// THere is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap = AggregateHelper.cumulateInNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).addAggregation(DOUBLE_FIRSY_KEY).build(), new SingleColumnAggregationLogic(
							DOUBLE_FIRSY_KEY, IDoubleBinaryOperator.SUM));
			Assertions.assertThat(resultAsMap).hasSize(1);

			// https://github.com/joel-costigliola/assertj-core/issues/315
			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(DOUBLE_FIRST_VALUE, resultAsMap.get(new TreeMap<>()).doubleValue(), 0.001D);
		}
	}
}
