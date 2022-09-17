package eu.solven.holymolap.performance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.measures.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.cube.measures.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.stable.v1.pojo.AggregatedAxis;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class MediumCardinalityDimensionTest {
	protected static final Logger LOGGER = LoggerFactory.getLogger(MediumCardinalityDimensionTest.class);

	@Test
	public void testOneHighCardinality() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		final int nbRows = 1_000_000;
		final float cardinalityFactor = 1;
		final int nbAxis = 100;
		final int nbDoubleAxis = 5;

		Set<String> keys = new LinkedHashSet<>();
		for (int i = 0; i < nbAxis; i++) {
			keys.add("Key_" + i);
		}

		Set<String> doubleKeys = new LinkedHashSet<>();
		for (int i = 0; i < nbDoubleAxis; i++) {
			doubleKeys.add("Double_" + i);
		}

		final Int2ObjectMap<IntSet> axisToValues = new Int2ObjectOpenHashMap<IntSet>();
		for (int axisIndex = 0; axisIndex < nbAxis; axisIndex++) {
			int expectedSize = 1 + (int) (axisIndex * cardinalityFactor);
			axisToValues.put(axisIndex, new IntLinkedOpenHashSet(expectedSize));
		}

		Iterator<IHolyRecord> rowIterator =
				makeRowsIterator(nbRows, cardinalityFactor, nbAxis, nbDoubleAxis, axisToValues);

		ImmutableSinkContext context = new ImmutableSinkContext(keys, doubleKeys, Collections.emptySet());
		IHolyCube cube = sink.sinkDeprecated(context, rowIterator);

		Assert.assertEquals(nbRows, cube.getNbRows());
		Assert.assertEquals(nbAxis + nbDoubleAxis, cube.getCellSet().getAxesWithCoordinates().axes().size());

		List<String> keyIterator = new ArrayList<>(keys);
		List<String> doubleIterator = new ArrayList<>(doubleKeys);

		for (int axisIndex = 0; axisIndex < nbAxis; axisIndex++) {

			{
				String axis = keyIterator.get(axisIndex);
				String doubleKey = doubleIterator.get(axisIndex % doubleIterator.size());

				long start = System.currentTimeMillis();

				final AtomicInteger resultSize = new AtomicInteger();
				AggregateHelper.consumeQueryResult(cube,
						AggregateQueryBuilder.wildcard(axis)
								.addAggregation(new AggregatedAxis(doubleKey, OperatorFactory.SUM))
								.build(),
						param -> resultSize.incrementAndGet());

				Assert.assertEquals(axisToValues.get(axisIndex).size(), resultSize.get());

				LOGGER.info("It took {} ms for {} aggregates for key={}",
						System.currentTimeMillis() - start,
						resultSize,
						axis);
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

				Assert.assertTrue(axisToValues.get(axisIndex).size() <= resultSize.get());

				LOGGER.info("It took {} ms for {} aggregates for wildcards: {}",
						System.currentTimeMillis() - start,
						resultSize,
						subKeys);
			}
		}
	}

	private Iterator<IHolyRecord> makeRowsIterator(final int nbRows,
			final float cardinalityFactor,
			final int nbAxis,
			final int nbDoubleAxis,
			final Int2ObjectMap<IntSet> axisToValues) {
		final int[] values = new int[nbAxis];
		final double[] doubles = new double[nbDoubleAxis];
		// final ArrayIndexedMap<String, Comparable<?>> buffer = new
		// ArrayIndexedMap<>(keys, values);

		final FastEntry reused = new FastEntry(Arrays.asList(), new Object[0], doubles, values);

		Iterator<IHolyRecord> rows = new AbstractIterator<IHolyRecord>() {
			int rowIndex = 0;

			@Override
			protected IHolyRecord computeNext() {
				if (rowIndex < nbRows) {
					rowIndex++;

					for (int i = 0; i < nbAxis; i++) {
						int itemIndex = 1 + (int) (i * cardinalityFactor);

						values[i] = ThreadLocalRandom.current().nextInt(itemIndex);
						axisToValues.get(i).add(values[i]);
					}

					for (int i = 0; i < nbDoubleAxis; i++) {
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
