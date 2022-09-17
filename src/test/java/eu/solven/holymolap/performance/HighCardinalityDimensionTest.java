package eu.solven.holymolap.performance;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.ILazyHolyCube;
import eu.solven.holymolap.cube.measures.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.cube.measures.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.sink.record.IHolyRecord;

public class HighCardinalityDimensionTest {

	@Test
	public void testOneHighCardinality() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		final int cardinality = 10000000;

		final Comparable<?>[] values = new Comparable<?>[2];

		final FastEntry reused = new FastEntry(Arrays.asList(TestAggregation.FIRST_KEY,
				TestAggregation.DOUBLE_FIRSY_KEY), values, new double[0], new int[0]);

		Iterator<IHolyRecord> rows = new AbstractIterator<IHolyRecord>() {
			int rowIndex = 0;

			@Override
			protected IHolyRecord computeNext() {
				if (rowIndex < cardinality) {

					// Each row has as value the rownIndex
					values[0] = rowIndex;
					values[1] = ThreadLocalRandom.current().nextDouble();

					rowIndex++;

					return reused;
				} else {
					return endOfData();
				}
			}
		};

		ImmutableSinkContext context =
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
						Collections.emptySet(),
						Collections.emptySet());
		IHolyCube cube = sink.sinkDeprecated(context, rows);

		Assert.assertEquals(cardinality, cube.getNbRows());
		Assert.assertEquals(2, cube.getCellSet().getAxesWithCoordinates().axes().size());

		if (cube instanceof ILazyHolyCube) {
			ILazyHolyCube lazyCube = (ILazyHolyCube) cube;
			lazyCube.getCellSet().startIndexing(Collections.singleton(TestAggregation.FIRST_KEY));
		}
	}

	@Test
	public void testTwoHighCardinality() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		final int cardinality = 1000000;

		final Comparable<?>[] values = new Comparable<?>[3];
		// final ArrayIndexedMap<String, Comparable<?>> buffer = new
		// ArrayIndexedMap<>(
		// ImmutableSet.of(TestAggregation.FIRST_KEY,
		// TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
		// values);

		final FastEntry reused = new FastEntry(ImmutableList.of(TestAggregation.FIRST_KEY,
				TestAggregation.SECOND_KEY,
				TestAggregation.DOUBLE_FIRSY_KEY), values, new double[0], new int[0]);

		Iterator<IHolyRecord> rows = new AbstractIterator<IHolyRecord>() {
			int rowIndex = 0;

			@Override
			protected IHolyRecord computeNext() {
				if (rowIndex < cardinality) {
					// Each row has as value the rownIndex
					values[0] = rowIndex;
					values[1] = rowIndex;
					values[2] = ThreadLocalRandom.current().nextDouble();

					rowIndex++;

					return reused;
				} else {
					return endOfData();
				}
			}
		};

		ImmutableSinkContext context = new ImmutableSinkContext(
				ImmutableSet
						.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
				Collections.emptySet(),
				Collections.emptySet());
		IHolyCube cube = sink.sinkDeprecated(context, rows);

		Assert.assertEquals(cardinality, cube.getNbRows());
		Assert.assertEquals(3, cube.getCellSet().getAxesWithCoordinates().axes().size());

		if (cube instanceof ILazyHolyCube) {
			ILazyHolyCube lazyCube = (ILazyHolyCube) cube;
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY));
		}
	}

	@Test
	public void testIndexAllKeysOneByOne() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		ImmutableSinkContext context = new ImmutableSinkContext(
				ImmutableSet
						.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
				Collections.emptySet(),
				Collections.emptySet());
		IHolyCube cube = sink.sink(context,
				new FastEntry(ImmutableList
						.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
						new Object[] { "a", "b", "c" }));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(3, cube.getCellSet().getAxesWithCoordinates().axes().size());

		if (cube instanceof ILazyHolyCube) {
			ILazyHolyCube lazyCube = (ILazyHolyCube) cube;
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.DOUBLE_FIRSY_KEY));
		}
	}
}
