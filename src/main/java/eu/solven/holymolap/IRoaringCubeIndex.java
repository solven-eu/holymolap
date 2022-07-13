package eu.solven.holymolap;

import java.util.List;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

public interface IRoaringCubeIndex {

	int NOT_INDEXED = -1;
	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	void startIndexing(int keyIndex);

	void startIndexing(Set<?> keysToIndex);

	long getSizeInBytes();

	// IntList getRowToValueIndex(int keyIndex);
	// IntList getRowToValueIndex(Object key);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(Object
	// wildcardKey);

	RoaringBitmap getValueIndexToBitmap(int wildcardKeyIndex, int valueIndex);

	RoaringBitmap getBitmap(Object wildcardKey, Object value);

	Object convertValueIndexToValue(Object key, int valueIndex);

	// Slow
	Object getValueAtRow(Object key, int row);

	List<?> getValuesForKey(String key);

	Set<?> keySet();

	int getKeyIndex(Object wildcardKey);

	Object getKeyAtIndex(int keyIndex);

	int getValueIndex(int wildcardKeyIndex, int rowToConsider);

}
