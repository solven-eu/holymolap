package eu.solven.holymolap.aggregate;

import org.roaringbitmap.RoaringBitmap;

public class RawCoordinatesToBitmap {
	public final long[] axisIndexToValueIndex;
	public final RoaringBitmap matchingRows;

	public RawCoordinatesToBitmap(RoaringBitmap matchingRows, long[] valueIndexes) {
		this.matchingRows = matchingRows;
		this.axisIndexToValueIndex = valueIndexes;
	}

}
