package eu.solven.holymolap.aggregate;

import org.roaringbitmap.RoaringBitmap;

public class RawCoordinatesToBitmap {
	final RoaringBitmap matchingRows;
	final long[] valueRefs;

	public RawCoordinatesToBitmap(RoaringBitmap matchingRows, long[] valueRefs) {
		this.matchingRows = matchingRows;
		this.valueRefs = valueRefs;
	}

	public RoaringBitmap getMatchingRows() {
		return matchingRows;
	}

	public long[] getCoordinateRefs() {
		return valueRefs;
	}

}
