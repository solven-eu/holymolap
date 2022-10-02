package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

/**
 * A column of aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableObjectAggregatesColumn implements IScannableMeasureColumn {
	final ObjectList<Object> cellToAggregate;
	final Object neutral;

	/**
	 * 
	 * @param cellToAggregate
	 * @param neutral
	 *            may be returned while iterating over rows without an aggregate
	 */
	public ImmutableObjectAggregatesColumn(ObjectList<Object> cellToAggregate, Object neutral) {
		this.cellToAggregate = cellToAggregate;
		this.neutral = neutral;
	}

	@Override
	public long getRows() {
		return cellToAggregate.size();
	}

	@Override
	public void acceptAggregates(Consumer<? super Object> aggregateConsumer) {
		cellToAggregate.forEach(aggregateConsumer);
	}

	@Override
	public Iterator<Object> map(LongIterator cellsIterator) {
		return new AbstractObjectIterator<Object>() {

			@Override
			public boolean hasNext() {
				return cellsIterator.hasNext();
			}

			@Override
			public Object next() {
				long row = cellsIterator.nextLong();

				if (row >= cellToAggregate.size() || row < 0L) {
					return neutral;
				}

				return cellToAggregate.get(Ints.checkedCast(row));
			}
		};
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		if (cellToAggregate instanceof ObjectArrayList) {
			sizeInBytes += 8 * ((ObjectArrayList<?>) cellToAggregate).elements().length;
		} else {
			sizeInBytes += 8 * cellToAggregate.size();
		}

		return sizeInBytes;
	}

}
