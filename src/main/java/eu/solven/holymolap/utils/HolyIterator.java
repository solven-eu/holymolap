package eu.solven.holymolap.utils;

import org.roaringbitmap.PeekableIntIterator;

import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * Helps working with iterators
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyIterator {

	public static LongIterator toLongIterator(PeekableIntIterator intIterator) {
		return new LongIterator() {

			@Override
			public boolean hasNext() {
				return intIterator.hasNext();
			}

			@Override
			public long nextLong() {
				return intIterator.next();
			}
		};
	}

}
