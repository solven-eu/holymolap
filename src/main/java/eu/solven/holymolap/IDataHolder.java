package eu.solven.holymolap;

import org.roaringbitmap.RoaringBitmap;

/**
 * An {@link IDataHolder} represents a raw column-oriented data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated()
public interface IDataHolder {
	/**
	 * 
	 * @param keyIndex
	 * @return the number of different values hold by a key
	 */
	long getAxisCardinality(int keyIndex);

	/**
	 * 
	 * @param axisIndex
	 * @param coordinateRef
	 * @return the rowIndexes matching given coordinate for given axis
	 */
	RoaringBitmap getCoordinateToBitmap(int axisIndex, long coordinateRef);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(int keyIndex);

	long getSizeInBytes();

}
