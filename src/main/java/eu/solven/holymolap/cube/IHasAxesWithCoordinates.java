package eu.solven.holymolap.cube;

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
	 * @param coordinateIndex
	 * @return the coordinate associated to given reference.
	 */
	Object dereferenceCoordinate(int axisIndex, long coordinateIndex);

	long getCoordinateRef(int axisIndex, Object coordinate);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the existing reference of given coordinate
	 */
	default long getCoordinateRef(String axis, Object coordinate) {
		return getCoordinateRef(getAxisIndex(axis), coordinate);
	}
}
