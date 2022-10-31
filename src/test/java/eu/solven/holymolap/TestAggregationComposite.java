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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasureTableDefinition;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public class TestAggregationComposite extends ATestAggregation implements IHolyMapDataTestConstants {

	@Test
	public void testEmptyCube() {
		HolyCube cube1 = new HolyCube();
		HolyCube cube2 = new HolyCube();
		CompositeHolyCube composite = new CompositeHolyCube(
				ImmutableMap.<String, IHolyCube>builder().put("1", cube1).put("2", cube2).build());

		Assertions.assertThat(composite.getNbRows()).isEqualTo(0);
		Assertions.assertThat(composite.getMeasuresDefinition().measures()).isEmpty();

		for (IAggregationQuery query : getAllQueries()) {
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(composite,
							query.addAggregations(new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM)));

			assertEmptyOrNeutral(result, 0D);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(composite, GRAND_TOTAL_COUNT);

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.values()).singleElement().isEqualTo(0L);
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_sameCell() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);

		IHolyCube cube1;
		{
			IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube1 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();
		}
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}

		CompositeHolyCube cube = new CompositeHolyCube(
				ImmutableMap.<String, IHolyCube>builder().put("1", cube1).put("2", cube2).build());

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		AggregateHelper.singleMeasureToNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis))
				.values()
				.forEach(aggregate -> {
					Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
				});

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
			});
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_differentCell() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);

		IHolyCube cube1;
		{
			IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube1 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();
		}
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
					new Object[] { SECOND_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}

		CompositeHolyCube cube = new CompositeHolyCube(
				ImmutableMap.<String, IHolyCube>builder().put("1", cube1).put("2", cube2).build());

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(measuredAxis));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
			});
		}

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
			});
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_oneIsEmpty() {
		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);

		HolyCube cube1 = new HolyCube();
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.of(measuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}

		CompositeHolyCube cube = new CompositeHolyCube(
				ImmutableMap.<String, IHolyCube>builder().put("1", cube1).put("2", cube2).build());

		Assertions.assertThat(cube.getNbRows()).isEqualTo(1);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(measuredAxis));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_SECOND_VALUE);
			});
		}

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_SECOND_VALUE);
			});
		}
	}
}
