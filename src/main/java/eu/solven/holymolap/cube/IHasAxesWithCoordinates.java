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

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the existing reference of given coordinate
	 */
	long getCoordinateRef(String axis, Object coordinate);
}
