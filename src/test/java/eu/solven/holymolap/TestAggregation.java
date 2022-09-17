package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.Arrays;
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
import eu.solven.holymolap.cube.aggregates.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.cube.aggregates.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.query.operator.IStandardOperators;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.ISinkContext;
import eu.solven.holymolap.sink.ObjectOnlySinkContext;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.pojo.AggregatedAxis;

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

	@Test
	public void testEmptyCube() {
		HolyCube cube = new HolyCube();

		Assert.assertEquals(0, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(),
					AggregateHelper.cumulateInNavigableMap(cube,
							query.addAggregations(new AggregatedAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM))));
		}
	}

	@Test
	public void testAddOneEmptyEntry() {
		ObjectOnlySinkContext context = new ObjectOnlySinkContext(new String[] {});

		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		IHolyCube cube = sink.sink(context, EmptyHolyRecord.INSTANCE);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			Assert.assertEquals(new TreeMap<>(),
					AggregateHelper.cumulateInNavigableMap(cube,
							query.addAggregations(
									() -> Arrays.asList(new AggregatedAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM)))));
		}
	}

	@Test
	public void testAddOneEntryAggregateNotDoubleKey() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube = sink.sink(new ObjectOnlySinkContext(new String[] { FIRST_KEY }),
				new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		Assert.assertEquals(new TreeMap<>(),
				AggregateHelper.cumulateInNavigableMap(cube,
						new EmptyAggregationQuery()
								.addAggregations(new AggregatedAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM))));

		// There is no double on FirstKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap =
					AggregateHelper.cumulateInNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
									.sum(FIRST_KEY)
									.build()
									.addAggregations(new AggregatedAxis(FIRST_KEY, OperatorFactory.SUM)));
			Assertions.assertThat(resultAsMap).hasSize(1);

			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(IStandardOperators.SUM.neutral(), resultAsMap.get(new TreeMap<>()), 0.001D);
		}
	}

	@Test
	public void testAddOneEntryAggregateDoubleKey() {
		ISinkContext context = new ObjectOnlySinkContext(new String[] { FIRST_KEY, DOUBLE_FIRSY_KEY });

		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube = sink.sink(context,
				new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE }));

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY, DOUBLE_FIRSY_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		Assert.assertEquals(new TreeMap<>(),
				AggregateHelper.cumulateInNavigableMap(cube,
						new EmptyAggregationQuery()
								.addAggregations(new AggregatedAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM))));

		// THere is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ? extends Double> resultAsMap =
					AggregateHelper.cumulateInNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
									.sum(DOUBLE_FIRSY_KEY)
									.build()
									.addAggregations(new AggregatedAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM)));
			Assertions.assertThat(resultAsMap).hasSize(1);

			// https://github.com/joel-costigliola/assertj-core/issues/315
			Assert.assertTrue(resultAsMap.containsKey(new TreeMap<>()));
			Assert.assertEquals(DOUBLE_FIRST_VALUE, resultAsMap.get(new TreeMap<>()).doubleValue(), 0.001D);
		}
	}
}
