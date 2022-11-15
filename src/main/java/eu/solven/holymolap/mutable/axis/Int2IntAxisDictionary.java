package eu.solven.holymolap.mutable.axis;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;
import it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntSets;

public class Int2IntAxisDictionary implements IMutableAxisSmallIntDictionary {
	protected final Int2IntMap coordinateToIndex;
	protected final AtomicBoolean locked = new AtomicBoolean();

	/**
	 * 
	 * @param coordinateToIndex
	 *            This has to be a Linked data-structure, as it will be returned by .orderedCoordinates()
	 */
	protected Int2IntAxisDictionary(Int2IntMap coordinateToIndex) {
		this.coordinateToIndex = coordinateToIndex;

		int defaultReturnValue = coordinateToIndex.defaultReturnValue();
		if (NO_COORDINATE_INDEX != defaultReturnValue) {
			throw new IllegalArgumentException("Illegal defaultReturnValue: " + defaultReturnValue);
		}
	}

	public Int2IntAxisDictionary() {
		this.coordinateToIndex = new Int2IntLinkedOpenHashMap();
		this.coordinateToIndex.defaultReturnValue(NO_COORDINATE_INDEX);
	}

	@Override
	public Set<?> orderedCoordinates() {
		locked.set(true);

		return IntSets.unmodifiable(coordinateToIndex.keySet());
	}

	@Override
	public synchronized int cardinality() {
		return coordinateToIndex.size();
	}

	@Override
	public synchronized int getIntIndexMayMiss(int coordinate) {
		return coordinateToIndex.get(coordinate);
	}

	@Override
	public synchronized int getIntIndexMayAppend(int coordinate) {
		if (locked.get()) {
			throw new IllegalStateException("Should not mutate once locked");
		}

		int coordinateIndex = getIntIndexMayMiss(coordinate);

		if (coordinateIndex == IAxisSmallDictionary.NO_COORDINATE_INDEX) {
			int previousCardinality = coordinateToIndex.size();
			// This is the first row with given coordinate
			coordinateToIndex.put(coordinate, previousCardinality);

			coordinateIndex = previousCardinality;
		}

		return coordinateIndex;
	}

	@Override
	public boolean isLocked() {
		return locked.get();
	}
}
