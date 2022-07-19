package eu.solven.holymolap.performance;

import java.util.ArrayList;
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
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.IFastEntry;
import eu.solven.holymolap.sink.IHolySink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.RoaringSink;
import eu.solven.holymolap.stable.v1.pojo.AggregatedAxis;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class MediumCardinalityDimensionTest {
	protected static final Logger LOGGER = LoggerFactory.getLogger(MediumCardinalityDimensionTest.class);

	@Test
	public void testOneHighCardinality() {
		IHolySink sink = new RoaringSink();

		final int nbRows = 1000000;
		final float cardinalityFactor = 1;
		final int nbKeys = 100;
		final int nbDouble = 5;

		Set<String> keys = new LinkedHashSet<>();
		for (int i = 0; i < nbKeys; i++) {
			keys.add("Key_" + i);
		}

		Set<String> doubleKeys = new LinkedHashSet<>();
		for (int i = 0; i < nbDouble; i++) {
			doubleKeys.add("Double_" + i);
		}

		final int[] values = new int[nbKeys];
		final double[] doubles = new double[nbDouble];
		// final ArrayIndexedMap<String, Comparable<?>> buffer = new
		// ArrayIndexedMap<>(keys, values);

		final Int2ObjectMap<IntSet> keyToValues = new Int2ObjectOpenHashMap<IntSet>();
		for (int i = 0; i < nbKeys; i++) {
			keyToValues.put(i, new IntLinkedOpenHashSet(1 + (int) (i * cardinalityFactor)));
		}

		final FastEntry reused = new FastEntry(new Object[0], doubles, values);

		Iterator<IFastEntry> rows = new AbstractIterator<IFastEntry>() {
			int rowIndex = 0;

			@Override
			protected IFastEntry computeNext() {
				if (rowIndex < nbRows) {
					rowIndex++;

					for (int i = 0; i < nbKeys; i++) {
						int itemIndex = 1 + (int) (i * cardinalityFactor);

						values[i] = ThreadLocalRandom.current().nextInt(itemIndex);
						keyToValues.get(i).add(values[i]);
					}

					for (int i = 0; i < nbDouble; i++) {
						doubles[i] = ThreadLocalRandom.current().nextDouble();
					}

					return reused;
				} else {
					return endOfData();
				}
			}
		};

		IHolyCube cube = sink.sink(rows, new ImmutableSinkContext(keys, doubleKeys, Collections.emptySet()));

		Assert.assertEquals(nbRows, cube.getNbRows());
		Assert.assertEquals(nbKeys + nbDouble, cube.getIndex().keySet().size());

		List<String> keyIterator = new ArrayList<>(keys);
		List<String> doubleIterator = new ArrayList<>(doubleKeys);

		for (int i = 0; i < nbKeys; i++) {

			{
				String key = keyIterator.get(i);
				String doubleKey = doubleIterator.get(i % doubleIterator.size());

				long start = System.currentTimeMillis();

				final AtomicInteger resultSize = new AtomicInteger();
				AggregateHelper.consumeQueryResult(cube,
						AggregateQueryBuilder.wildcard(key)
								.addAggregation(new AggregatedAxis(doubleKey, OperatorFactory.SUM))
								.build(),
						param -> resultSize.incrementAndGet());

				Assert.assertEquals(keyToValues.get(i).size(), resultSize.get());

				LOGGER.info("It took {} ms for {} aggregates for key={}",
						System.currentTimeMillis() - start,
						resultSize,
						key);
			}

			{
				// Keep 5 keys
				List<String> subKeys = keyIterator.subList(Math.max(0, i - 5), i + 1);
				String doubleKey = doubleIterator.get(i % doubleIterator.size());

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

				Assert.assertTrue(keyToValues.get(i).size() <= resultSize.get());

				LOGGER.info("It took {} ms for {} aggregates for wildcards: {}",
						System.currentTimeMillis() - start,
						resultSize,
						subKeys);
			}
		}
	}
}
