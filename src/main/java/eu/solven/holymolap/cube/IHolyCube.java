package eu.solven.holymolap.cube;

import java.util.Collection;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.IRoaringCubeIndex;
import eu.solven.holymolap.stable.v1.IHasColumns;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

/**
 * An {@link IHolyCube} is the multi-dimentional data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCube extends IHasMemoryFootprint {
	Collection<? extends RoaringBitmap> getAxesBitmaps(IHasColumns columns);

	RoaringBitmap getFiltersBitmap(IHasFilters hasFilters);

	IRoaringCubeIndex getIndex();

	long getNbRows();

	// RoaringBitmap getAllRows();

	// NavigableSet<? > keySet();

	// NavigableMap<?, ?> convertToCoordinates(int row, Set<?> keys);

	String indexToColumn(int keyIndex);

	// <T> T aggregate(RoaringBitmap matchingRows, AggregationLogic<T>
	// aggregationLogic);

	// List<?> getValuesForKey(String key);

	DoubleIterator readDouble(IntIterator it, int keyIndex, double defaultValue);

	// Iterator<RawCoordinatesToBitmap> aggregate(Set<? >
	// wildcards, RoaringBitmap filteredRows);

}
