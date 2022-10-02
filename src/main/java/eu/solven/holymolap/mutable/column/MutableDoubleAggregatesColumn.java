package eu.solven.holymolap.mutable.column;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;
import eu.solven.holymolap.immutable.column.ImmutableDoubleAggregatesColumn;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A column of double aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableDoubleAggregatesColumn implements IMutableDoubleAggregatesColumn {
	final IDoubleBinaryOperator operator;
	final DoubleList cellToAggregate;

	final AtomicLong brokenRows = new AtomicLong();

	final AtomicBoolean hasBeenRead = new AtomicBoolean();

	final AtomicBoolean flushed = new AtomicBoolean();

	protected MutableDoubleAggregatesColumn(IDoubleBinaryOperator operator, final DoubleList cellToAggregate) {
		this.operator = operator;
		this.cellToAggregate = cellToAggregate;
	}

	public MutableDoubleAggregatesColumn(IDoubleBinaryOperator operator) {
		this.operator = operator;
		this.cellToAggregate = new DoubleArrayList();
	}

	@Override
	public synchronized void aggregateDouble(int rowIndex, double contribution) {
		if (rowIndex < 0) {
			throw new IllegalArgumentException("rowIndex must be positive: " + rowIndex);
		}

		ensureCapacity(rowIndex);

		double previousAggregate = cellToAggregate.getDouble(rowIndex);
		double newAggregate = operator.applyAsDouble(previousAggregate, contribution);
		cellToAggregate.set(rowIndex, newAggregate);
	}

	private void ensureCapacity(int rowIndex) {
		int initialSize = cellToAggregate.size();
		if (initialSize <= rowIndex) {
			// The underlying array is too small for given row
			cellToAggregate.size(rowIndex + 1);

			double neutral = operator.neutralAsDouble();
			for (int i = initialSize; i < rowIndex; i++) {
				cellToAggregate.set(i, neutral);
			}
		}
	}

	@Override
	public IScannableDoubleMeasureColumn flush() {
		if (!flushed.compareAndSet(false, true)) {
			throw new IllegalStateException("Already flushed");
		}
		return new ImmutableDoubleAggregatesColumn(cellToAggregate, operator.neutralAsDouble());
	}

}
