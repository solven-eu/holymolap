package eu.solven.holymolap.mutable.column;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.compression.doubles.DynamicSchemeDoubleList;
import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;
import eu.solven.holymolap.immutable.column.ImmutableDoubleAggregatesColumn;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import eu.solven.pepper.memory.IPepperMemoryConstants;
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
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		if (cellToAggregate instanceof IHasMemoryFootprint) {
			sizeInBytes += ((IHasMemoryFootprint) cellToAggregate).getSizeInBytes();
		} else if (cellToAggregate instanceof DoubleArrayList) {
			sizeInBytes += IPepperMemoryConstants.DOUBLE * ((DoubleArrayList) cellToAggregate).elements().length;
		} else {
			sizeInBytes += IPepperMemoryConstants.DOUBLE * cellToAggregate.size();
		}

		return sizeInBytes;
	}

	@Override
	public synchronized void aggregateDouble(int rowIndex, double contribution) {
		if (rowIndex < 0) {
			throw new IllegalArgumentException("rowIndex must be positive: " + rowIndex);
		}

		if (contribution == operator.neutralAsDouble()) {
			// No need to contribute neutral element. It may help skipping unnecessary capacity
			return;
		}

		ensureCapacity(rowIndex);

		double previousAggregate = cellToAggregate.getDouble(rowIndex);
		double newAggregate = operator.applyAsDouble(previousAggregate, contribution);
		cellToAggregate.set(rowIndex, newAggregate);
	}

	private void ensureCapacity(int rowIndex) {
		int initialSize = cellToAggregate.size();
		if (initialSize == rowIndex) {
			double neutral = operator.neutralAsDouble();
			cellToAggregate.add(neutral);
		} else if (initialSize < rowIndex) {
			// The underlying array is too small for given row
			if (cellToAggregate instanceof DoubleArrayList) {
				((DoubleArrayList) cellToAggregate).ensureCapacity(initialSize);
			}

			// BEWARE do not rely on .size as it will force underlying to given size
			// cellToAggregate.size(rowIndex + 1);

			double neutral = operator.neutralAsDouble();

			for (int i = initialSize; i <= rowIndex; i++) {
				cellToAggregate.add(neutral);
			}
		}
	}

	@Override
	public IScannableDoubleMeasureColumn flush() {
		if (!flushed.compareAndSet(false, true)) {
			throw new IllegalStateException("Already flushed");
		}
		DynamicSchemeDoubleList dynamicScheme = new DynamicSchemeDoubleList(cellToAggregate);
		return new ImmutableDoubleAggregatesColumn(dynamicScheme, operator.neutralAsDouble());
	}

}
