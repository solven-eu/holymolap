package eu.solven.holymolap.mutable.column;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.immutable.column.IScannableLongMeasureColumn;
import eu.solven.holymolap.immutable.column.ImmutableLongAggregatesColumn;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * A column of double aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableLongAggregatesColumn implements IMutableLongAggregatesColumn {
	final ILongBinaryOperator operator;
	final LongList cellToAggregate;

	final AtomicLong brokenRows = new AtomicLong();

	final AtomicBoolean hasBeenRead = new AtomicBoolean();

	final AtomicBoolean flushed = new AtomicBoolean();

	protected MutableLongAggregatesColumn(ILongBinaryOperator operator, final LongList cellToAggregate) {
		this.operator = operator;
		this.cellToAggregate = cellToAggregate;
	}

	public MutableLongAggregatesColumn(ILongBinaryOperator operator) {
		this.operator = operator;
		this.cellToAggregate = new LongArrayList();
	}

	@Override
	public synchronized void aggregateLong(int rowIndex, long contribution) {
		if (rowIndex < 0) {
			throw new IllegalArgumentException("rowIndex must be positive: " + rowIndex);
		}

		ensureCapacity(rowIndex);

		long previousAggregate = cellToAggregate.getLong(rowIndex);
		long newAggregate = operator.applyAsLong(previousAggregate, contribution);
		cellToAggregate.set(rowIndex, newAggregate);
	}

	private void ensureCapacity(int rowIndex) {
		int initialSize = cellToAggregate.size();
		if (initialSize <= rowIndex) {
			// The underlying array is too small for given row
			cellToAggregate.size(rowIndex + 1);

			long neutral = operator.neutralAsLong();
			for (int i = initialSize; i < rowIndex; i++) {
				cellToAggregate.set(i, neutral);
			}
		}
	}

	@Override
	public IScannableLongMeasureColumn flush() {
		if (!flushed.compareAndSet(false, true)) {
			throw new IllegalStateException("Already flushed");
		}
		return new ImmutableLongAggregatesColumn(cellToAggregate, operator.neutralAsLong());
	}

}
