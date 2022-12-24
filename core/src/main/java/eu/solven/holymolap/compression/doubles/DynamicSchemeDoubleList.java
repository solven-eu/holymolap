package eu.solven.holymolap.compression.doubles;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.holymolap.cube.IMayCache;
import eu.solven.holymolap.immutable.column.ImmutableDoubleAggregatesColumn;
import eu.solven.holymolap.primitives.ICompactable;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * An immutable {@link DoubleList} which can be compressed (with {@link ICompactable}) through multiple schemes.
 * 
 * @author Benoit Lacelle
 *
 */
public class DynamicSchemeDoubleList extends AbstractDoubleList
		implements ICompactable, IMayCache, IHasMemoryFootprint {
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
	public long getSizeInBytes() {
		return ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(underlying.get());
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
		DoubleList initialUnderlying = currentUnderlying;

		long currentSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(currentUnderlying);

		if (!(currentUnderlying instanceof DictionaryDoubleList)) {
			Optional<DictionaryDoubleList> optLowDistincts =
					DictionaryDoubleList.tryMake(currentUnderlying.toDoubleArray());

			if (optLowDistincts.isPresent()) {
				DictionaryDoubleList lowDistincts = optLowDistincts.get();
				long candidateSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(lowDistincts);

				if (candidateSize < currentSize) {
					underlying.compareAndSet(currentUnderlying, lowDistincts);
					currentUnderlying = lowDistincts;
					currentSize = candidateSize;
				}
			}
		}

		if (!(currentUnderlying instanceof ReOrderVariableByteDoubleList)) {
			ReOrderVariableByteDoubleList reorderVariableByteList =
					new ReOrderVariableByteDoubleList(currentUnderlying.toDoubleArray());
			long candidateSize = ImmutableDoubleAggregatesColumn.estimateDoubleListFootprint(reorderVariableByteList);

			if (candidateSize < currentSize) {
				underlying.compareAndSet(currentUnderlying, reorderVariableByteList);
				currentUnderlying = reorderVariableByteList;
				currentSize = candidateSize;
			}
		}

		trimmed.set(true);

		DoubleList newUnderlying = underlying.get();
		if (newUnderlying instanceof IMayCache) {
			((IMayCache) newUnderlying).invalidateCache();
		}
		if (initialUnderlying.size() != newUnderlying.size()) {
			throw new IllegalStateException("The compression corrupted the length");
		}
		for (int i = 0; i < initialUnderlying.size(); i++) {
			double initialD = initialUnderlying.getDouble(i);
			double newD = newUnderlying.getDouble(i);

			// NaN are properly handled
			if (0 != Double.compare(initialD, newD)) {
				throw new IllegalStateException("The compression (" + newUnderlying.getClass()
						+ ") is not lossless. For index="
						+ i
						+ " "
						+ initialD
						+ " != "
						+ newD
						+ " initial double[]="
						+ initialUnderlying);
			}
		}
	}

	@Override
	public void invalidateCache() {
		DoubleList currentUnderlying = underlying.get();
		if (currentUnderlying instanceof IMayCache) {
			((IMayCache) currentUnderlying).invalidateCache();
		}
	}
}
