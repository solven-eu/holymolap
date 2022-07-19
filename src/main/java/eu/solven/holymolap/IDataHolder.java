package eu.solven.holymolap;

import org.roaringbitmap.RoaringBitmap;

public interface IDataHolder {
	/**
	 * 
	 * @param keyIndex 
	 * @return the number of different values hold by a key
	 */
	long getKeyCardinality(int keyIndex);

	RoaringBitmap getValueIndexToBitmap(int keyIndex, long valueIndex);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(int keyIndex);

	long getSizeInBytes();

}
