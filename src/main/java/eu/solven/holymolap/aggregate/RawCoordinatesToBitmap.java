package eu.solven.holymolap.aggregate;

import org.roaringbitmap.RoaringBitmap;

public class RawCoordinatesToBitmap {
	final CoordinatesRefs coordinates;
	final RoaringBitmap matchingRows;

	public RawCoordinatesToBitmap(CoordinatesRefs coordinates, RoaringBitmap matchingRows) {
		this.coordinates = coordinates;
		this.matchingRows = matchingRows;
	}

	/**
	 * 
	 * @return the coordinates of the considered cell
	 */
	public CoordinatesRefs getSlice() {
		return coordinates;
	}

	/**
	 * 
	 * @return the rowIndex matching current cell
	 */
	public RoaringBitmap getMatchingRows() {
		return matchingRows;
	}

}
