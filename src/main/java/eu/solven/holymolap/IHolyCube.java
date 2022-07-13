package eu.solven.holymolap;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

public interface IHolyCube {
	Collection<? extends RoaringBitmap> getKeyBitmaps(Collection<?> keys);

	Collection<RoaringBitmap> getValueBitmaps(Map<?, ?> filter);

	IRoaringCubeIndex getIndex();

	int getNbRows();

	// RoaringBitmap getAllRows();

	// NavigableSet<? > keySet();

	NavigableMap<?, ?> convertToCoordinates(int row, Set<?> keys);

	Object convertKeyIndexToKey(int keyIndex);

	// <T> T aggregate(RoaringBitmap matchingRows, AggregationLogic<T>
	// aggregationLogic);

	// List<?> getValuesForKey(String key);

	long getSizeInBytes();

	DoubleIterator readDouble(IntIterator it, int keyIndex, double defaultValue);

	// Iterator<RawCoordinatesToBitmap> aggregate(Set<? >
	// wildcards, RoaringBitmap filteredRows);

}
