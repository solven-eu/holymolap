package eu.solven.holymolap.cube.index;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

public interface IHolyCubeIndex extends IHasMemoryFootprint, IHasAxesWithCoordinates {

	long NOT_INDEXED = -1;
	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	RoaringBitmap getValueIndexToBitmap(int axisIndex, long coordinateIndex);

	default RoaringBitmap getBitmap(String axis, Object coordinate) {
		int axisIndex = getAxisIndex(axis);
		return getValueIndexToBitmap(axisIndex, getCoordinateRef(axis, coordinate));
	}

	long getCoordinateIndex(int axisIndex, long rowToConsider);

}
