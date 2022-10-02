package eu.solven.holymolap.mutable.column;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.immutable.column.IScannableMeasureColumn;
import eu.solven.holymolap.immutable.column.ImmutableObjectAggregatesColumn;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

/**
 * A column of double aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableAggregatesColumn implements IMutableAggregatesColumn {
	final IBinaryOperator operator;
	final ObjectList<Object> cellToAggregate;

	final AtomicLong brokenRows = new AtomicLong();

	final AtomicBoolean hasBeenRead = new AtomicBoolean();

	final AtomicBoolean flushed = new AtomicBoolean();

	protected MutableAggregatesColumn(IBinaryOperator operator, final ObjectList<Object> cellToAggregate) {
		this.operator = operator;
		this.cellToAggregate = cellToAggregate;
	}

	public MutableAggregatesColumn(IBinaryOperator operator) {
		this.operator = operator;
		this.cellToAggregate = new ObjectArrayList<>();
	}

	@Override
	public synchronized void aggregateObject(int rowIndex, Object contribution) {
		if (rowIndex < 0) {
			throw new IllegalArgumentException("rowIndex must be positive: " + rowIndex);
		}

		ensureCapacity(rowIndex);

		Object previousAggregate = cellToAggregate.get(rowIndex);
		Object newAggregate = operator.apply(previousAggregate, contribution);
		cellToAggregate.set(rowIndex, newAggregate);
	}

	private void ensureCapacity(int rowIndex) {
		int initialSize = cellToAggregate.size();
		if (initialSize <= rowIndex) {
			// The underlying array is too small for given row
			cellToAggregate.size(rowIndex + 1);

			Object neutral = operator.neutral();
			for (int i = initialSize; i < rowIndex; i++) {
				cellToAggregate.set(i, neutral);
			}
		}
	}

	@Override
	public IScannableMeasureColumn flush() {
		if (!flushed.compareAndSet(false, true)) {
			throw new IllegalStateException("Already flushed");
		}
		return new ImmutableObjectAggregatesColumn(cellToAggregate, operator.neutral());
	}

}
