package eu.solven.holymolap;

import java.util.List;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

public interface IRoaringCubeIndex {

	int NOT_INDEXED = -1;
	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	void startIndexing(int keyIndex);

	void startIndexing(Set<String> keysToIndex);

	long getSizeInBytes();

	// IntList getRowToValueIndex(int keyIndex);
	// IntList getRowToValueIndex(Object key);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(Object
	// wildcardKey);

	RoaringBitmap getValueIndexToBitmap(int wildcardKeyIndex, int valueIndex);

	RoaringBitmap getBitmap(String wildcardKey, Object value);

	Object convertValueIndexToValue(String key, int valueIndex);

	// Slow
	Object getValueAtRow(String key, int row);

	List<?> getValuesForKey(String key);

	Set<?> keySet();

	int getKeyIndex(String wildcardKey);

	String getKeyAtIndex(int keyIndex);

	int getValueIndex(int wildcardKeyIndex, int rowToConsider);

}
