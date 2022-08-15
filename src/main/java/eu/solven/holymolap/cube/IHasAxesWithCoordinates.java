package eu.solven.holymolap.cube;

import org.roaringbitmap.RoaringBitmap;

/**
 * Read-only operations
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasAxesWithCoordinates extends IHasAxes {

	/**
	 * 
	 * @param axisIndex
	 * @param coordinateRef
	 * @return the coordinate associated to given reference.
	 */
	Object dereferenceCoordinate(int axisIndex, long coordinateRef);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the index of given coordinate, or -1.
	 */
	long getCoordinateRef(int axisIndex, Object coordinate);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the existing reference of given coordinate
	 */
	default long getCoordinateRef(String axis, Object coordinate) {
		int axisIndex = getAxisIndex(axis);

		return getCoordinateRef(axisIndex, coordinate);
	}
}
