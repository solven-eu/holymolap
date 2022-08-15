package eu.solven.holymolap.cube.index;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * {@link IHolyCellSet} dedicates to storing the cells (i.e. the coordinates of a cube, not its aggregates).
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCellSet extends IHasMemoryFootprint, IHasAxesWithCoordinates {

	long NOT_INDEXED = -1;
	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the cellIndexes matching given coordinate
	 */
	RoaringBitmap getCoordinateToBitmap(int axisIndex, long coordinateIndex);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the cellIndexes matching given coordinate
	 */
	default RoaringBitmap getCoordinateToBitmap(String axis, Object coordinate) {
		int axisIndex = getAxisIndex(axis);
		if (axisIndex < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		}
		long coordinateRef = getCoordinateRef(axis, coordinate);
		return getCoordinateToBitmap(axisIndex, coordinateRef);
	}

	/**
	 * 
	 * @param cellIndex
	 *            the index of the cell to consider. A cell is defined by coordinate along all axes.
	 * @param axesIndexes
	 *            the axes for which we are interested.
	 * @return the coordinatesIndexes of given cell for given axes. The array has size size as input axesIndexes
	 */
	long[] getCellCoordinatesRef(long cellIndex, int... axesIndexes);

	@Deprecated
	default long getCellCoordinateRef(long cellIndex, int axisIndex) {
		return getCellCoordinatesRef(cellIndex, axisIndex)[0];
	}
}
