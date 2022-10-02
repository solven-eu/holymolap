package eu.solven.holymolap.mutable.axis;

import eu.solven.holymolap.mutable.cube.IHolyMutable;
import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;

/**
 * Bijection between coordinates and indexes (from 0, to the cardinality of the dictionary).
 * 
 * It accepts new coordinates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableAxisSmallDictionarySink extends IAxisSmallDictionary, IHolyMutable {
	// Marker used to inject missing coordinates in inputs not allowing null references.
	Object NO_COORDINATE = new Object();

	/**
	 * 
	 * @param coordinate
	 * @return the coordinate index, being the new cardinality in case of insertion.
	 */
	int getIndexMayAppend(Object coordinate);
}
