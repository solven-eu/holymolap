package eu.solven.holymolap.immutable.column;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.holymolap.compression.doubles.DictionaryDoubleList;
import eu.solven.holymolap.compression.doubles.ReOrderVariableByteDoubleColumn;
import eu.solven.holymolap.primitives.ICompactable;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * 
 * @author Benoit Lacelle
 *
 */
public class DynamicSchemeDoubleList extends AbstractDoubleList implements ICompactable, IHasMemoryFootprint {
	protected final AtomicReference<DoubleList> underlying = new AtomicReference<>();
	protected final AtomicBoolean trimmed = new AtomicBoolean(false);

	public DynamicSchemeDoubleList() {
		underlying.set(DoubleArrayList.of());
	}

	public DynamicSchemeDoubleList(DoubleList doubles) {
		underlying.set(doubles);
	}

	public DynamicSchemeDoubleList(double[] doubles) {
		this(new DoubleArrayList(doubles));
	}

	@Override
	public double getDouble(int index) {
		return underlying.get().getDouble(index);
	}

	@Override
	public int size() {
		return underlying.get().size();
	}

	@Override
	public void trim() {
		if (trimmed.get()) {
			// Once trimmed, as this is immutable, there is no point in trimming again
			return;
		}

		DoubleList currentUnderlying = underlying.get();

		long currentSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(currentUnderlying);

		if (!(currentUnderlying instanceof DictionaryDoubleList)) {
			DictionaryDoubleList lowDistincts = new DictionaryDoubleList(currentUnderlying.toDoubleArray());
			long candidateSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(lowDistincts);

			if (candidateSize < currentSize) {
				underlying.compareAndSet(currentUnderlying, lowDistincts);
				currentSize = candidateSize;
			}
		}

		if (!(currentUnderlying instanceof ReOrderVariableByteDoubleColumn)) {
			ReOrderVariableByteDoubleColumn reorderVariableByteList =
					new ReOrderVariableByteDoubleColumn(currentUnderlying.toDoubleArray());
			long candidateSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(reorderVariableByteList);

			if (candidateSize < currentSize) {
				underlying.compareAndSet(currentUnderlying, reorderVariableByteList);
				currentSize = candidateSize;
			}
		}

		trimmed.set(true);
	}
}
