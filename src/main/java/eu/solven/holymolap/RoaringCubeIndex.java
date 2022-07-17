package eu.solven.holymolap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.sink.IKeyValuesIndex;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import javolution.util.function.Consumer;

@ManagedResource
public class RoaringCubeIndex implements IRoaringCubeIndex {

	protected final int nbRows;
	protected final List<String> keyIndexToKey;

	protected final List<IntList> keyToRowToValueIndex;
	protected final List<IntList> keysBeingIndexed;

	protected final List<? extends IKeyValuesIndex> keyIndexToValueIndex;

	protected final IDataHolder dataHolder;

	public RoaringCubeIndex(int nbRows,
			List<String> keyIndexToKey,
			List<? extends IKeyValuesIndex> keyIndexToValueIndex,
			IDataHolder dataHolder) {
		this.nbRows = nbRows;
		this.keyIndexToKey = keyIndexToKey;

		int nbKeys = keyIndexToKey.size();

		this.keyToRowToValueIndex = Arrays.asList(new IntList[nbKeys]);
		this.keysBeingIndexed = Arrays.asList(new IntList[nbKeys]);

		this.keyIndexToValueIndex = keyIndexToValueIndex;
		this.dataHolder = dataHolder;
	}

	@Override
	public int getKeyIndex(String key) {
		return keyIndexToKey.indexOf(key);
	}

	@Override
	public void startIndexing(Set<String> keysToIndex) {
		for (String keyToIndex : keysToIndex) {
			int keyIndex = getKeyIndex(keyToIndex);

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
					int[] rawIndex = new int[nbRows];
					Arrays.fill(rawIndex, NOT_INDEXED);

					IntList valuesIndex = new IntArrayList(rawIndex);
					keysBeingIndexed.set(keyIndex, valuesIndex);

					new PositionIndexBuilder().buildIndex(keyIndex, valuesIndex, dataHolder, new Consumer<Integer>() {

						@Override
						public void accept(Integer indexedKey) {
							onIndexingCompleted(indexedKey);
						}
					});
				}
			}
		}
	}

	protected void onIndexingCompleted(int indexedKey) {
		synchronized (keyToRowToValueIndex) {
			// This key has finished being indexed
			IntList index = keysBeingIndexed.get(indexedKey);

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

		for (IntList primitives : keyToRowToValueIndex) {
			if (primitives instanceof IntArrayList) {
				sizeInBytes += 4 * ((IntArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 4 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public int getValueIndex(int keyIndex, int rowToConsider) {
		if (keyIndex < 0) {
			return NOT_INDEXED;
		} else {
			IntList rowToValueIndex = keyToRowToValueIndex.get(keyIndex);

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

			return rowToValueIndex.getInt(rowToConsider);
		}
	}

	@Override
	public RoaringBitmap getValueIndexToBitmap(int keyIndex, int valueIndex) {
		return dataHolder.getValueIndexToBitmap(keyIndex, valueIndex);
	}

	@Override
	public Object convertValueIndexToValue(String key, int valueIndex) {
		return keyIndexToValueIndex.get(getKeyIndex(key)).getValue(valueIndex);
	}

	@Override
	public RoaringBitmap getBitmap(String key, Object value) {
		int keyIndex = getKeyIndex(key);

		if (keyIndex < 0) {
			return EMPTY_BITMAP;
		}

		IKeyValuesIndex keyValuesIndex = keyIndexToValueIndex.get(keyIndex);

		int valueIndex = keyValuesIndex.getValueIndex(value);

		return getValueIndexToBitmap(keyIndex, valueIndex);
	}

	// Slow
	@Override
	public Object getValueAtRow(String key, int row) {
		int keyIndex = getKeyIndex(key);

		// TODO: check keyBitmap

		// Slow
		for (int valueIndex = 0; valueIndex < dataHolder.getKeyCardinality(keyIndex); valueIndex++) {
			if (dataHolder.getValueIndexToBitmap(keyIndex, valueIndex).contains(row)) {

				IKeyValuesIndex keyValuesIndex = keyIndexToValueIndex.get(keyIndex);
				return keyValuesIndex.getValue(valueIndex);
			}
		}

		// We found no matching value
		return null;
	}

	@Override
	public List<?> getValuesForKey(String key) {
		return keyIndexToValueIndex.get(getKeyIndex(key)).values();
	}

	@Override
	public Set<?> keySet() {
		return new HashSet<>(this.keyIndexToKey);
	}

	@Override
	public String getKeyAtIndex(int keyIndex) {
		return keyIndexToKey.get(keyIndex);
	}

}
