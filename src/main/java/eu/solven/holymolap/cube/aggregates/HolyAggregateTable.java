package eu.solven.holymolap.cube.aggregates;

import java.util.List;
import java.util.function.DoubleConsumer;

import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyAggregateTable implements IHolyAggregateTable {

	/**
	 * For each key, gives the value as double, which would be valid only where the key-bitmap is true
	 */
	protected final List<? extends DoubleList> axisIndexToDoubles;

	public HolyAggregateTable(List<? extends DoubleList> axisIndexToDoubles) {
		this.axisIndexToDoubles = axisIndexToDoubles;
	}

	@Override
	public DoubleIterator readDouble(final LongIterator rowsIterator, final int axisIndex) {
		if (axisIndex < 0) {
			return DoubleIterators.EMPTY_ITERATOR;
		} else if (axisIndex >= axisIndexToDoubles.size()) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		DoubleList doubles = axisIndexToDoubles.get(axisIndex);
		if (doubles == null) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		return new AbstractDoubleIterator() {

			@Override
			public boolean hasNext() {
				return rowsIterator.hasNext();
			}

			@Override
			public double nextDouble() {
				long row = rowsIterator.nextLong();

				return doubles.getDouble(Ints.checkedCast(row));
			}
		};
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (DoubleList primitives : axisIndexToDoubles) {
			if (primitives instanceof DoubleArrayList) {
				sizeInBytes += 8 * ((DoubleArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 8 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public void acceptDoubles(LongIterator rowsIterator, int axisIndex, DoubleConsumer doubleConsumer) {
		readDouble(rowsIterator, axisIndex).forEachRemaining(doubleConsumer);
	}
}
