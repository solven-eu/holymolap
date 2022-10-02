package eu.solven.holymolap.immutable.column;

import java.util.Set;
import java.util.function.LongConsumer;

import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * A column of aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableLongAggregatesColumn implements IScannableLongMeasureColumn {
	final LongList cellToAggregate;
	final long neutral;

	/**
	 * 
	 * @param cellToAggregate
	 * @param neutral
	 *            may be returned while iterating over rows without an aggregate
	 */
	public ImmutableLongAggregatesColumn(LongList cellToAggregate, long neutral) {
		this.cellToAggregate = cellToAggregate;
		this.neutral = neutral;
	}

	@Override
	public long getRows() {
		return cellToAggregate.size();
	}

	@Override
	public void acceptAggregates(LongConsumer aggregateConsumer) {
		cellToAggregate.forEach(aggregateConsumer);
	}

	@Override
	public LongIterator mapToLong(LongIterator cellsIterator) {
		return new AbstractLongIterator() {

			@Override
			public boolean hasNext() {
				return cellsIterator.hasNext();
			}

			@Override
			public long nextLong() {
				long row = cellsIterator.nextLong();

				if (row >= cellToAggregate.size() || row < 0L) {
					return neutral;
				}

				return cellToAggregate.getLong(Ints.checkedCast(row));
			}
		};
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		if (cellToAggregate instanceof LongArrayList) {
			sizeInBytes += 8 * ((LongArrayList) cellToAggregate).elements().length;
		} else {
			sizeInBytes += 8 * cellToAggregate.size();
		}

		return sizeInBytes;
	}

}
