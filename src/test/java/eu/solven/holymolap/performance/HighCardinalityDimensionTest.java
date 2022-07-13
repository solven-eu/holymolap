package eu.solven.holymolap.performance;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.IHolyCube;
import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.IFastEntry;
import eu.solven.holymolap.sink.IRoaringSink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.RoaringSink;

public class HighCardinalityDimensionTest {

	@Test
	public void testOneHighCardinality() {
		IRoaringSink sink = new RoaringSink();

		final int cardinality = 10000000;

		final Comparable<?>[] values = new Comparable<?>[2];

		final FastEntry reused = new FastEntry(values, new double[0], new int[0]);

		Iterator<IFastEntry> rows = new AbstractIterator<IFastEntry>() {
			int rowIndex = 0;

			@Override
			protected IFastEntry computeNext() {
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

		IHolyCube cube = sink.sink(rows, new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
				Collections.emptySet(), Collections.emptySet()));

		// final ArrayIndexedMap<String, Comparable<?>> buffer = new
		// ArrayIndexedMap<>(
		// ImmutableSet.of(TestAggregation.FIRST_KEY,
		// TestAggregation.DOUBLE_FIRSY_KEY), values);

		// Iterator<Map<? extends Comparable<?>, ? extends Comparable<?>>> rows
		// = new Iterator<Map<? extends Comparable<?>, ? extends
		// Comparable<?>>>() {
		// int rowIndex = 0;
		//
		// @Override
		// public boolean hasNext() {
		// return rowIndex < cardinality;
		// }
		//
		// @Override
		// public Map<? extends Comparable<?>, ? extends Comparable<?>> next() {
		// int currentRow = rowIndex;
		// rowIndex++;
		//
		// // Each row has as value the rownIndex
		// buffer.putIndexed(0, currentRow);
		// buffer.putIndexed(1, ThreadLocalRandom.current().nextDouble());
		//
		// return buffer;
		// }
		//
		// @Override
		// public void remove() {
		// throw new UnsupportedOperationException();
		// }
		// };

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(cardinality, cube.getNbRows());
		Assert.assertEquals(2, cube.getIndex().keySet().size());

		cube.getIndex().startIndexing(Collections.singleton(TestAggregation.FIRST_KEY));
	}

	@Test
	public void testTwoHighCardinality() {
		IRoaringSink sink = new RoaringSink();

		final int cardinality = 1000000;

		final Comparable<?>[] values = new Comparable<?>[3];
		// final ArrayIndexedMap<String, Comparable<?>> buffer = new
		// ArrayIndexedMap<>(
		// ImmutableSet.of(TestAggregation.FIRST_KEY,
		// TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
		// values);

		final FastEntry reused = new FastEntry(values, new double[0], new int[0]);

		Iterator<IFastEntry> rows = new AbstractIterator<IFastEntry>() {
			int rowIndex = 0;

			@Override
			protected IFastEntry computeNext() {
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

		IHolyCube cube = sink.sink(rows,
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
						Collections.emptySet(), Collections.emptySet()));

		// Iterator<Map<? extends Comparable<?>, ? extends Comparable<?>>> rows
		// = new Iterator<Map<? extends Comparable<?>, ? extends
		// Comparable<?>>>() {
		// int rowIndex = 0;
		//
		// @Override
		// public boolean hasNext() {
		// return rowIndex < cardinality;
		// }
		//
		// @Override
		// public Map<? extends Comparable<?>, ? extends Comparable<?>> next() {
		// int currentRow = rowIndex;
		// rowIndex++;
		//
		// buffer.putIndexed(0, currentRow);
		// buffer.putIndexed(1, currentRow);
		// buffer.putIndexed(2, ThreadLocalRandom.current().nextDouble());
		//
		// return buffer;
		// }
		//
		// @Override
		// public void remove() {
		// throw new UnsupportedOperationException();
		// }
		// };

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(cardinality, cube.getNbRows());
		Assert.assertEquals(3, cube.getIndex().keySet().size());

		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY));
	}

	@Test
	public void testIndexAllKeysOneByOne() {
		IRoaringSink sink = new RoaringSink();

		IHolyCube cube = sink.sink(new FastEntry(new Object[] { "a", "b", "c" }),
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY, TestAggregation.DOUBLE_FIRSY_KEY),
						Collections.emptySet(), Collections.emptySet()));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(3, cube.getIndex().keySet().size());

		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));
		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.DOUBLE_FIRSY_KEY));
	}
}
