package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.measures.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.cube.measures.HolyMeasureTableDefinition;
import eu.solven.holymolap.cube.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.query.operator.IStandardOperators;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public class TestAggregation {
	public static final String FIRST_KEY = "firstKey";
	public static final String FIRST_VALUE = "firstValue";

	public static final String SECOND_KEY = "secondKey";
	public static final String SECOND_VALUE = "secondValue";

	public static final String DOUBLE_FIRSY_KEY = "doubleFirstKey";
	public static final double DOUBLE_FIRST_VALUE = 13D;

	public static final String DOUBLE_SECOND_KEY = "doubleSecondKey";
	public static final double DOUBLE_SECOND_VALUE = 17D;

	public static IAggregationQuery GRAND_TOTAL = new EmptyAggregationQuery();

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

	private void assertEmptyOrNeutral(NavigableMap<? extends NavigableMap<?, ?>, ?> result, Object neutral) {
		if (result.isEmpty()) {
			// The measures has no contributing rows: OK
		} else {
			// Everything equals the neutral
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(neutral);
			});
		}
	}

	@Test
	public void testEmptyCube() {
		HolyCube cube = new HolyCube();

		Assert.assertEquals(0, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.cumulateInNavigableMap(cube,
					query.addAggregations(new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM)));

			assertEmptyOrNeutral(result, 0D);
		}
	}

	@Test
	public void testAddOneEmptyEntry() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = new HolyMeasureTableDefinition(Arrays.asList(measuredAxis));
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		IHolyCube cube = sink.sink(EmptyHolyRecord.INSTANCE);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			Map<NavigableMap<?, ?>, Object> empty = new TreeMap<>(NavigableMapComparator.INSTANCE);
			empty.put(Collections.emptyNavigableMap(), 0D);

			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.cumulateInNavigableMap(cube,
					query.addAggregations(() -> Arrays.asList(measuredAxis)));

			assertEmptyOrNeutral(result, 0D);
		}
	}

	@Test
	public void testAddOneEntryAggregateNotDoubleKey() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);
		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		assertEmptyOrNeutral(
				AggregateHelper.cumulateInNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis)),
				0D);

		// There is no double on FirstKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.cumulateInNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
							.build()
							.addAggregations(new MeasuredAxis(FIRST_KEY, OperatorFactory.SUM)));

			assertEmptyOrNeutral(result, 0D);
		}
	}

	@Test
	public void testAddOneEntryAggregateDoubleKey() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
				new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY, DOUBLE_FIRSY_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		assertEmptyOrNeutral(
				AggregateHelper.cumulateInNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis)),
				0D);

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.cumulateInNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

			assertEmptyOrNeutral(result, 0D);
		}
	}
}
