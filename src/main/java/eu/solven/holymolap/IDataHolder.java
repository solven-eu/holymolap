package eu.solven.holymolap;

import org.roaringbitmap.RoaringBitmap;

/**
 * An {@link IDataHolder} represents a raw column-oriented data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated(sin)
public interface IDataHolder {
	/**
	 * 
	 * @param keyIndex
	 * @return the number of different values hold by a key
	 */
	long getKeyCardinality(int keyIndex);

	/**
	 * 
	 * @param axisIndex
	 * @param valueIndex
	 * @return the bitmap of positions for given (Axis, Coordinate)
	 */
	RoaringBitmap getValueIndexToBitmap(int axisIndex, long valueIndex);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(int keyIndex);

	long getSizeInBytes();

}
