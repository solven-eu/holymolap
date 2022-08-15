package eu.solven.holymolap.cube.mutable;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A column of double aggregates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableAggregatesColumn implements IMutableDoubleAggregatesColumn {
	final IDoubleBinaryOperator operator;
	final DoubleList rowToAggregate;

	final AtomicLong brokenRows = new AtomicLong();

	final AtomicBoolean hasBeenRead = new AtomicBoolean();

	protected MutableAggregatesColumn(IDoubleBinaryOperator operator, final DoubleList rowToAggregate) {
		this.operator = operator;
		this.rowToAggregate = rowToAggregate;
	}

	public MutableAggregatesColumn(IDoubleBinaryOperator operator) {
		this.operator = operator;
		this.rowToAggregate = new DoubleArrayList();
	}

	@Override
	public synchronized void aggregateRow(int rowIndex, double contribution) {
		if (rowIndex < 0) {
			throw new IllegalArgumentException("rowIndex must be positive: " + rowIndex);
		}

		ensureCapacity(rowIndex);

		double previousAggregate = rowToAggregate.getDouble(rowIndex);
		double newAggregate = operator.applyAsDouble(previousAggregate, contribution);
		rowToAggregate.set(rowIndex, newAggregate);
	}

	private void ensureCapacity(int rowIndex) {
		int initialSize = rowToAggregate.size();
		if (initialSize <= rowIndex) {
			// The underlying array is too small for given row
			rowToAggregate.size(rowIndex + 1);

			double neutral = operator.neutral();
			for (int i = initialSize; i < rowIndex; i++) {
				rowToAggregate.set(i, neutral);
			}
		}
	}

}
