package eu.solven.holymolap.immutable.table;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * An {@link IHolyDictionarizedTable} represents a raw column-oriented data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyDictionarizedTable extends IHasMemoryFootprint {
	/**
	 * 
	 * @param axesIndexes
	 *            the considered axes
	 * @param valuesRefs
	 *            the coordinate ref for each axis
	 * @return the cellIndexes matching given coordinates on given axes.
	 */
	RoaringBitmap getCoordinateToRows(int[] axesIndexes, long[] valuesRefs);

	/**
	 * 
	 * @param axis
	 * @param coordinate
	 * @return the cellIndexes matching given coordinate on given axis.
	 */
	@Deprecated
	default RoaringBitmap getCoordinateToRows(int axisIndex, long coordinateRef) {
		return getCoordinateToRows(new int[] { axisIndex }, new long[] { coordinateRef });
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
