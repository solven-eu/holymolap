package eu.solven.holymolap.cube.table;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.exception.HolyExceptionManagement;

public class EmptyHolyDictionarizedTable implements IHolyDictionarizedTable {
	@Override
	public long getSizeInBytes() {
		return 0;
	}

	// @Override
	// public long getAxisCardinality(int axisIndex) {
	// return -1;
	// }

	@Override
	public RoaringBitmap getCoordinateToRows(int[] axesIndexes, long[] valuesRefs) {
		return HolyExceptionManagement.immutableEmptyBitmap();
	}

	@Override
	public long[] getCellCoordinates(long cellIndex, int[] axesIndexes) {
		throw new IllegalArgumentException("Empty");
	}

}
