package eu.solven.holymolap.aggregate;

import java.util.NavigableMap;

import org.roaringbitmap.RoaringBitmap;

public class Aggregate {
	public final RoaringBitmap matchingRows;
	public final NavigableMap<? extends Comparable<?>, ? extends Comparable<?>> coordinates;

	public Aggregate(RoaringBitmap matchingRows, NavigableMap<? extends Comparable<?>, ? extends Comparable<?>> coordinates) {
		this.matchingRows = matchingRows;
		this.coordinates = coordinates;
	}

}
