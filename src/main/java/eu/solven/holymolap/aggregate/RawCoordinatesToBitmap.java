package eu.solven.holymolap.aggregate;

import org.roaringbitmap.RoaringBitmap;

public class RawCoordinatesToBitmap {
	public final int[] valueIndexes;
	public final RoaringBitmap matchingRows;

	public RawCoordinatesToBitmap(RoaringBitmap matchingRows, int[] valueIndexes) {
		this.matchingRows = matchingRows;
		this.valueIndexes = valueIndexes;
	}

}
