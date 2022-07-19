package eu.solven.holymolap;

import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

public interface IHolyCubeIndex extends IHasMemoryFootprint, IHasAxesWithCoordinates {

	long NOT_INDEXED = -1;
	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	void startIndexing(int axisIndex);

	void startIndexing(Set<String> keysToIndex);

	RoaringBitmap getValueIndexToBitmap(int axisIndex, long coordinateIndex);

	RoaringBitmap getBitmap(String axis, Object coordinate);

	// Slow
	// Object getValueAtRow(String axis, long row);

	// List<?> getValuesForKey(String axis);

	long getCoordinateIndex(int axisIndex, long rowToConsider);

}
