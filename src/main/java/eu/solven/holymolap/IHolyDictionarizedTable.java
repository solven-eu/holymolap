package eu.solven.holymolap;

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
	 * @param axisIndex
	 * @return the number of different values hold by an axis
	 */
	long getAxisCardinality(int axisIndex);

	/**
	 * 
	 * @param axisIndex
	 * @param coordinateRef
	 * @return the rowIndexes matching given coordinate for given axis
	 */
	RoaringBitmap getCoordinateToRows(int axisIndex, long coordinateRef);

	long[] getCellCoordinates(long cellIndex, int[] axesIndexes);

}
