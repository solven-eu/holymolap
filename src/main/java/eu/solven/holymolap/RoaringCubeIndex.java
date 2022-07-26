package eu.solven.holymolap;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.roaringbitmap.RoaringBitmap;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.primitives.Ints;

import eu.solven.holymolap.cube.index.ILazyHolyCubeIndex;
import eu.solven.holymolap.sink.IKeyValuesIndex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

@ManagedResource
public class RoaringCubeIndex implements ILazyHolyCubeIndex {

	protected final long nbRows;
	protected final List<? extends String> keyIndexToKey;

	protected final List<LongList> keyToRowToValueIndex;
	protected final List<LongList> keysBeingIndexed;

	protected final List<? extends IKeyValuesIndex> keyIndexToValueIndex;

	protected final IDataHolder dataHolder;

	public RoaringCubeIndex(int nbRows,
			List<? extends String> keyIndexToKey,
			List<? extends IKeyValuesIndex> keyIndexToValueIndex,
			IDataHolder dataHolder) {
		this.nbRows = nbRows;
		this.keyIndexToKey = keyIndexToKey;

		int nbKeys = keyIndexToKey.size();

		this.keyToRowToValueIndex = Arrays.asList(new LongList[nbKeys]);
		this.keysBeingIndexed = Arrays.asList(new LongList[nbKeys]);

		this.keyIndexToValueIndex = keyIndexToValueIndex;
		this.dataHolder = dataHolder;
	}

	@Override
	public int getAxisIndex(String key) {
		return keyIndexToKey.indexOf(key);
	}

	@Override
	public void startIndexing(Set<String> keysToIndex) {
		for (String keyToIndex : keysToIndex) {
			int keyIndex = getAxisIndex(keyToIndex);

			startIndexing(keyIndex);
		}
	}

	@Override
	public void startIndexing(int keyIndex) {
		if (keyToRowToValueIndex.get(keyIndex) == null) {
			synchronized (keyToRowToValueIndex) {
				// Check if there is no index yet, and it is not being
				// computed
				if (keyToRowToValueIndex.get(keyIndex) == null && keysBeingIndexed.get(keyIndex) == null) {
					long[] rawIndex = new long[Ints.checkedCast(nbRows)];
					Arrays.fill(rawIndex, NOT_INDEXED);

					LongList valuesIndex = new LongArrayList(rawIndex);
					keysBeingIndexed.set(keyIndex, valuesIndex);

					indexBuilder().buildIndex(keyIndex,
							dataHolder,
							valuesIndex,
							indexedKey -> onIndexingCompleted(indexedKey));
				}
			}
		}
	}

	protected PositionIndexBuilder indexBuilder() {
		return new PositionIndexBuilder();
	}

	protected void onIndexingCompleted(int indexedKey) {
		synchronized (keyToRowToValueIndex) {
			// This key has finished being indexed
			LongList index = keysBeingIndexed.get(indexedKey);

			// Put in keyToRowToValueIndex BEFORE removing from
			// keysBeingIndexed, else startIndexing could believe this key
			// is not indexed
			keyToRowToValueIndex.set(indexedKey, index);
			keysBeingIndexed.set(indexedKey, null);
		}
	}

	@ManagedAttribute
	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (LongList primitives : keyToRowToValueIndex) {
			if (primitives instanceof LongArrayList) {
				sizeInBytes += 4 * ((LongArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 4 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public long getCoordinateIndex(int keyIndex, long rowToConsider) {
		if (keyIndex < 0) {
			return NOT_INDEXED;
		} else {
			LongList rowToValueIndex = keyToRowToValueIndex.get(keyIndex);

			if (rowToValueIndex == null) {
				startIndexing(keyIndex);

				// TODO: a proper notify mechanism
				while (keyToRowToValueIndex.get(keyIndex) == null) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				rowToValueIndex = keyToRowToValueIndex.get(keyIndex);
			}

			return rowToValueIndex.getLong(Ints.checkedCast(rowToConsider));
		}
	}

	@Override
	public RoaringBitmap getValueIndexToBitmap(int keyIndex, long valueIndex) {
		return dataHolder.getValueIndexToBitmap(keyIndex, valueIndex);
	}

	@Override
	public Object dereferenceCoordinate(int axisIndex, long valueIndex) {
		return keyIndexToValueIndex.get(axisIndex).getValue(valueIndex);
	}

	@Override
	public long getCoordinateRef(int axisIndex, Object coordinate) {
		return keyIndexToValueIndex.get(axisIndex).getValueIndex(coordinate);
	}

	@Override
	public RoaringBitmap getBitmap(String key, Object value) {
		int keyIndex = getAxisIndex(key);

		if (keyIndex < 0) {
			return EMPTY_BITMAP;
		}

		IKeyValuesIndex keyValuesIndex = keyIndexToValueIndex.get(keyIndex);
		long valueIndex = keyValuesIndex.getValueIndex(value);

		return getValueIndexToBitmap(keyIndex, valueIndex);
	}

	// Slow
	// @Override
	// public Object getValueAtRow(String key, long row) {
	// int keyIndex = getKeyIndex(key);
	//
	// // TODO: check keyBitmap
	//
	// // Slow
	// for (int valueIndex = 0; valueIndex < dataHolder.getKeyCardinality(keyIndex); valueIndex++) {
	// if (dataHolder.getValueIndexToBitmap(keyIndex, valueIndex).contains(Ints.checkedCast(row))) {
	//
	// IKeyValuesIndex keyValuesIndex = keyIndexToValueIndex.get(keyIndex);
	// return keyValuesIndex.getValue(valueIndex);
	// }
	// }
	//
	// // We found no matching value
	// return null;
	// }

	// @Override
	// public List<?> getValuesForKey(String key) {
	// return keyIndexToValueIndex.get(getKeyIndex(key)).values();
	// }

	@Override
	public NavigableSet<String> keySet() {
		return new TreeSet<>(this.keyIndexToKey);
	}

	@Override
	public String indexToAxis(int keyIndex) {
		return keyIndexToKey.get(keyIndex);
	}

}
