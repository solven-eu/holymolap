package eu.solven.holymolap;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.exception.HolyExceptionManagement;

public class EmptyHolyDictionarizedTable implements IHolyDictionarizedTable {
	@Override
	public long getSizeInBytes() {
		return 0;
	}

	@Override
	public long getAxisCardinality(int axisIndex) {
		return -1;
	}

	@Override
	public RoaringBitmap getCoordinateToRows(int axisIndex, long coordinateRef) {
		return HolyExceptionManagement.immutableEmptyBitmap();
	}

	@Override
	public long[] getCellCoordinates(long cellIndex, int[] axesIndexes) {
		throw new IllegalArgumentException("Empty");
	}

}
