package eu.solven.holymolap;

import java.util.Arrays;
import java.util.Collections;
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
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.AggregationToMapHelper;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public class TestAggregation extends ATestAggregation implements IHolyMapDataTestConstants {

	@Test
	public void testEmptyCube() {
		HolyCube cube = new HolyCube();

		Assert.assertEquals(0, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(cube,
							query.addAggregations(new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM)));

			assertEmptyOrNeutral(result, 0D);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(cube, GRAND_TOTAL_COUNT);

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.values()).singleElement().isEqualTo(0L);
		}
	}

	@Test
	public void testAddOneEmptyEntry() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.withCountStar(Arrays.asList(measuredAxis));
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		IHolyCube cube = sink.sink(EmptyHolyRecord.INSTANCE).closeToHolyCube();

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(Collections.emptySet(), cube.getCellSet().getAxesWithCoordinates().axes());

		for (IAggregationQuery query : getAllQueries()) {
			Map<NavigableMap<?, ?>, Object> empty = new TreeMap<>(NavigableMapComparator.INSTANCE);
			empty.put(Collections.emptyNavigableMap(), 0D);

			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregationToMapHelper
					.singleMeasureToNavigableMap(cube, query.addAggregations(() -> Arrays.asList(measuredAxis)));

			assertEmptyOrNeutral(result, 0D);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(cube, GRAND_TOTAL_COUNT);

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.values()).singleElement().isEqualTo(1L);
		}
	}

	@Test
	public void testAddOneEntryAggregateNotDoubleKey() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(measuredAxis);
		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube =
				sink.sink(new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE })).closeToHolyCube();

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		assertEmptyOrNeutral(
				AggregationToMapHelper.singleMeasureToNavigableMap(cube,
						new EmptyAggregationQuery().addAggregations(measuredAxis)),
				0D);

		// There is no double on FirstKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
									.build()
									.addAggregations(new MeasuredAxis(FIRST_KEY, OperatorFactory.SUM)));

			assertEmptyOrNeutral(result, 0D);
		}
	}

	@Test
	public void testAddOneEntryAggregateDoubleKey() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(measuredAxis);

		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube cube = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
				new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(new TreeSet<>(ImmutableSet.of(FIRST_KEY, DOUBLE_FIRST_KEY)),
				cube.getCellSet().getAxesWithCoordinates().axes());

		AggregationToMapHelper
				.singleMeasureToNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis))
				.values()
				.forEach(aggregate -> {
					Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
				});

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(cube,
							AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
			});
		}
	}
}
