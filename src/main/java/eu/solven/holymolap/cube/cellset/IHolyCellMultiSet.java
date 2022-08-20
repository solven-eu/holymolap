package eu.solven.holymolap.cube.cellset;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * {@link IHolyCellMultiSet} dedicates to storing the cells (i.e. the coordinates of a cube, not its aggregates). It
 * should be seen as an injection from a long to a fine-grain cells (expressing as many axes as possible). 2 equivalent
 * cells may be mapped to different cellIndexes.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCellMultiSet extends IHasMemoryFootprint, IHasAxesWithCoordinates {

	RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the cellIndexes matching given coordinate on given axis.
	 */
	RoaringBitmap getCoordinateToCells(int axisIndex, long coordinateRef);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the cellIndexes matching given coordinate on given axis.
	 */
	default RoaringBitmap getCoordinateToBitmap(String axis, Object coordinate) {
		int axisIndex = getAxisIndex(axis);
		if (axisIndex < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		}
		long coordinateRef = getCoordinateRef(axis, coordinate);
		return getCoordinateToCells(axisIndex, coordinateRef);
	}

	/**
	 * 
	 * @param cellIndex
	 *            the index of the cell to consider. A cell is defined by coordinate along all axes.
	 * @param axesIndexes
	 *            the axes for which we are interested.
	 * @return the coordinatesRefs of given cell for given axes. The array has same size as input axesIndexes
	 */
	long[] getCellCoordinates(long cellIndex, int... axesIndexes);

	@Deprecated
	default long getCellCoordinateRef(long cellIndex, int axisIndex) {
		return getCellCoordinates(cellIndex, axisIndex)[0];
	}
}
