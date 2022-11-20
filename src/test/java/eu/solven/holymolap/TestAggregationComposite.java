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
import com.google.common.collect.ImmutableSortedMap;

import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
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

	final MeasuredAxis sumFirstKey = new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM);

	@Test
	public void testEmptyCube() {
		HolyCube cube1 = new HolyCube();
		HolyCube cube2 = new HolyCube();

		CompositeHolyCube cube = new CompositeHolyCube(cube1, cube2);

		Assertions.assertThat(cube.getNbRows()).isEqualTo(0);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).isEmpty();

		for (IAggregationQuery query : getAllQueries()) {
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(cube, query.addAggregations(sumFirstKey));

			assertEmptyOrNeutral(result, 0D);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(cube, GRAND_TOTAL_COUNT);

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.values()).singleElement().isEqualTo(0L);
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_sameCell() {
		IHolyCube cube1;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(sumFirstKey);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube1 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();
		}
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(sumFirstKey);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}
		CompositeHolyCube cube = new CompositeHolyCube(cube1, cube2);

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		AggregateHelper.singleMeasureToNavigableMap(cube, new EmptyAggregationQuery().addAggregations(sumFirstKey))
				.values()
				.forEach(aggregate -> {
					Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
				});

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(sumFirstKey));

			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
			});
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_differentCell() {
		IHolyCube cube1;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(sumFirstKey);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube1 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();
		}
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(sumFirstKey);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
					new Object[] { SECOND_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}
		CompositeHolyCube cube = new CompositeHolyCube(cube1, cube2);

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(sumFirstKey));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE + DOUBLE_SECOND_VALUE);
			});
		}

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(sumFirstKey));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
			});
		}
	}

	@Test
	public void testSingleMeasureIn2Cubes_oneIsEmpty() {
		HolyCube cube1 = new HolyCube();
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(sumFirstKey);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}

		CompositeHolyCube cube = new CompositeHolyCube(cube1, cube2);

		Assertions.assertThat(cube.getNbRows()).isEqualTo(1);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(1);

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(sumFirstKey));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_SECOND_VALUE);
			});
		}

		// There is a single fact for DoubleKey
		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(sumFirstKey));

			Assertions.assertThat(result).hasSize(1);
			result.values().forEach(aggregate -> {
				Assertions.assertThat(aggregate).isEqualTo(DOUBLE_SECOND_VALUE);
			});
		}
	}

	@Test
	public void testMultipleMeasureIn2Cubes() {
		MeasuredAxis firstMeasuredAxis = sumFirstKey;
		MeasuredAxis secondMeasuredAxis = new MeasuredAxis(DOUBLE_SECOND_KEY, OperatorFactory.SUM);
		MeasuredAxis thirdMeasuredAxis = new MeasuredAxis(DOUBLE_THIRD_KEY, OperatorFactory.SUM);

		IHolyCube cube1;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(firstMeasuredAxis, secondMeasuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube1 = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY, DOUBLE_SECOND_KEY),
					new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE, DOUBLE_SECOND_VALUE })).closeToHolyCube();
		}
		IHolyCube cube2;
		{
			IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(secondMeasuredAxis, thirdMeasuredAxis);

			IHolyCubeSink sink = new HolyCubeSink(definitions);
			cube2 = sink
					.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_SECOND_KEY, DOUBLE_THIRD_KEY),
							new Object[] { FIRST_VALUE, DOUBLE_SECOND_VALUE + 7D, DOUBLE_THIRD_VALUE }))
					.closeToHolyCube();
		}

		CompositeHolyCube cube = new CompositeHolyCube(cube1, cube2);

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getMeasuresDefinition().measures()).hasSize(3);

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(firstMeasuredAxis));

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.get(ImmutableSortedMap.of())).isEqualTo(DOUBLE_FIRST_VALUE);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(secondMeasuredAxis));

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.get(ImmutableSortedMap.of())).isEqualTo(DOUBLE_SECOND_VALUE * 2D + 7D);
		}

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
					new EmptyAggregationQuery().addAggregations(thirdMeasuredAxis));

			Assertions.assertThat(result).hasSize(1);
			Assertions.assertThat(result.get(ImmutableSortedMap.of())).isEqualTo(DOUBLE_THIRD_VALUE);
		}
	}
}
