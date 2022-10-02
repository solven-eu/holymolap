package eu.solven.holymolap.immutable.column;

import java.util.Set;
import java.util.function.DoubleConsumer;

import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * A column of aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableDoubleAggregatesColumn implements IScannableDoubleMeasureColumn {
	final DoubleList cellToAggregate;
	final double neutral;

	/**
	 * 
	 * @param cellToAggregate
	 * @param neutral
	 *            may be returned while iterating over rows without an aggregate
	 */
	public ImmutableDoubleAggregatesColumn(DoubleList cellToAggregate, double neutral) {
		this.cellToAggregate = cellToAggregate;
		this.neutral = neutral;
	}

	@Override
	public long getRows() {
		return cellToAggregate.size();
	}

	@Override
	public void acceptAggregates(DoubleConsumer aggregateConsumer) {
		cellToAggregate.forEach(aggregateConsumer);
	}

	@Override
	public DoubleIterator mapToDouble(LongIterator cellsIterator) {
		return new AbstractDoubleIterator() {

			@Override
			public boolean hasNext() {
				return cellsIterator.hasNext();
			}

			@Override
			public double nextDouble() {
				long row = cellsIterator.nextLong();

				if (row >= cellToAggregate.size() || row < 0L) {
					return neutral;
				}

				return cellToAggregate.getDouble(Ints.checkedCast(row));
			}
		};
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		if (cellToAggregate instanceof DoubleArrayList) {
			sizeInBytes += 8 * ((DoubleArrayList) cellToAggregate).elements().length;
		} else {
			sizeInBytes += 8 * cellToAggregate.size();
		}

		return sizeInBytes;
	}

}
