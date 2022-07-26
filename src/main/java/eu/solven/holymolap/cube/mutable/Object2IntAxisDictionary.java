package eu.solven.holymolap.cube.mutable;

import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

public class Object2IntAxisDictionary implements IMutableAxisDictionary {
	protected final Object2IntMap<Object> coordinateToIndex;
	protected final AtomicBoolean locked = new AtomicBoolean();

	protected Object2IntAxisDictionary(Object2IntMap<Object> coordinateToIndex) {
		this.coordinateToIndex = coordinateToIndex;

		int defaultReturnValue = coordinateToIndex.defaultReturnValue();
		if (NO_COORDINATE_INDEX != defaultReturnValue) {
			throw new IllegalArgumentException("Illegal defaultReturnValue: " + defaultReturnValue);
		}
	}

	public Object2IntAxisDictionary() {
		this.coordinateToIndex = new Object2IntLinkedOpenHashMap<>();
		this.coordinateToIndex.defaultReturnValue(NO_COORDINATE_INDEX);
	}

	@Override
	public Object2IntMap<Object> asObject2Int() {
		locked.set(true);

		return coordinateToIndex;
	}

	@Override
	public synchronized int cardinality() {
		return coordinateToIndex.size();
	}

	@Override
	public synchronized int getIndexMayMiss(Object coordinate) {
		return coordinateToIndex.getInt(coordinate);
	}

	@Override
	public synchronized int getIndexMayAppend(Object coordinate) {
		if (locked.get()) {
			throw new IllegalStateException("Should not mutate once locked");
		}

		int coordinateIndex = getIndexMayMiss(coordinate);

		if (coordinateIndex == IAxisDictionary.NO_COORDINATE_INDEX) {
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
