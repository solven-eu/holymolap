package eu.solven.holymolap;

import org.roaringbitmap.RoaringBitmap;

public interface IDataHolder {
	/**
	 * 
	 * @param keyIndex 
	 * @return the number of different values hold by a key
	 */
	int getKeyCardinality(int keyIndex);

	RoaringBitmap getValueIndexToBitmap(int keyIndex, int valueIndex);

	// List<? extends RoaringBitmap> getValueIndexToBitmap(int keyIndex);

	long getSizeInBytes();

}
