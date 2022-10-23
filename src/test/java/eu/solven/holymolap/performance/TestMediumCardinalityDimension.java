package eu.solven.holymolap.performance;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.sink.record.IHolyRecord;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public class TestMediumCardinalityDimension {
	protected static final Logger LOGGER = LoggerFactory.getLogger(TestMediumCardinalityDimension.class);

	@Test
	public void testOneHighCardinality() {
		IHolyMeasuresDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		final int nbRows = 100_000;
		final float cardinalityFactor = 1;
		final int nbAxis = 100;
		final int nbDoubleAxis = 2;

		Set<String> intAxes = new LinkedHashSet<>();
		for (int i = 0; i < nbAxis; i++) {
			intAxes.add("Key_" + i);
		}

		Set<String> doubleAxes = new LinkedHashSet<>();
		for (int i = 0; i < nbDoubleAxis; i++) {
			doubleAxes.add("Double_" + i);
		}

		final Int2ObjectMap<LongSet> axisToValues = new Int2ObjectOpenHashMap<>();
		for (int axisIndex = 0; axisIndex < nbAxis; axisIndex++) {
			int expectedSize = 1 + (int) (axisIndex * cardinalityFactor);
			axisToValues.put(axisIndex, new LongLinkedOpenHashSet(expectedSize));
		}

		List<String> recordAxes = new ArrayList<>();
		recordAxes.addAll(intAxes);
		recordAxes.addAll(doubleAxes);

		Iterator<IHolyRecord> rowIterator =
				makeRowsIterator(nbRows, cardinalityFactor, nbAxis, nbDoubleAxis, recordAxes, axisToValues);

		IHolyCube cube = sink.sinkDeprecated(rowIterator).closeToHolyCube();

		Assert.assertEquals(nbRows, cube.getNbRows());
		Assert.assertEquals(nbAxis + nbDoubleAxis, cube.getCellSet().getAxesWithCoordinates().axes().size());

		List<String> keyIterator = new ArrayList<>(intAxes);
		List<String> doubleIterator = new ArrayList<>(doubleAxes);

		for (int axisIndex = 0; axisIndex < nbAxis; axisIndex++) {
			checkAxis(axisToValues, cube, keyIterator, doubleIterator, axisIndex);
		}
	}

	private void checkAxis(final Int2ObjectMap<LongSet> axisToValues,
			IHolyCube cube,
			List<String> keyIterator,
			List<String> doubleIterator,
			int axisIndex) {
		{
			String axis = keyIterator.get(axisIndex);
			String doubleKey = doubleIterator.get(axisIndex % doubleIterator.size());

			long start = System.currentTimeMillis();

			final AtomicInteger resultSize = new AtomicInteger();
			AggregateHelper
					.consumeQueryResult(cube, AggregateQueryBuilder.wildcard(axis).sum(doubleKey).build(), param -> {
						resultSize.incrementAndGet();
					});

			LOGGER.info("It took {} ms for {} aggregates for key={}",
					System.currentTimeMillis() - start,
					resultSize,
					axis);

			Assertions.assertThat(axisToValues.get(axisIndex)).hasSize(resultSize.get());
		}

		{
			// Keep 5 keys
			List<String> subKeys = keyIterator.subList(Math.max(0, axisIndex - 5), axisIndex + 1);
			String doubleKey = doubleIterator.get(axisIndex % doubleIterator.size());

			long start = System.currentTimeMillis();

			final AtomicInteger resultSize = new AtomicInteger();
			AggregateHelper.consumeQueryResult(cube,
					AggregateQueryBuilder.wildcards(subKeys).addAggregation(OperatorFactory.sum(doubleKey)).build(),
					new Consumer<RawCoordinatesToBitmap>() {

						@Override
						public void accept(RawCoordinatesToBitmap param) {
							resultSize.incrementAndGet();
						}
					});

			LOGGER.info("It took {} ms for {} aggregates for wildcards: {}",
					System.currentTimeMillis() - start,
					resultSize,
					subKeys);

			Assertions.assertThat(axisToValues.get(axisIndex).size()).isLessThanOrEqualTo(resultSize.get());
		}
	}

	private Iterator<IHolyRecord> makeRowsIterator(final int nbRows,
			final float cardinalityFactor,
			final int nbIntAxes,
			final int nbDoubleAxes,
			List<String> recordAxes,
			final Int2ObjectMap<LongSet> axisToValues) {
		final long[] values = new long[nbIntAxes];
		final double[] doubles = new double[nbDoubleAxes];

		final FastEntry reused = new FastEntry(recordAxes, new Object[0], values, doubles);

		Iterator<IHolyRecord> rows = new AbstractIterator<IHolyRecord>() {
			int rowIndex = 0;

			@Override
			protected IHolyRecord computeNext() {
				if (rowIndex < nbRows) {
					rowIndex++;

					for (int i = 0; i < nbIntAxes; i++) {
						int itemIndex = 1 + (int) (i * cardinalityFactor);

						// Axis with higher index has higher cardinality
						values[i] = ThreadLocalRandom.current().nextLong(itemIndex);
						axisToValues.get(i).add(values[i]);
					}

					for (int i = 0; i < nbDoubleAxes; i++) {
						doubles[i] = ThreadLocalRandom.current().nextDouble();
					}

					return reused;
				} else {
					return endOfData();
				}
			}
		};
		return rows;
	}
}
