package eu.solven.holymolap.mutable.dictionary;

import java.util.Set;

/**
 * Bijection between coordinates and indexes (from 0, to the cardinality of the dictionary).
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAxisSmallDictionary {
	// -1 as coordinateIndex could be any positive integer
	int NO_COORDINATE_INDEX = -1;

	// Object2IntMap<Object> asObject2Int();

	int cardinality();

	int getIndexMayMiss(Object coordinate);

	/**
	 * 
	 * @return a Set of coordinates, while iteration while go from ref=0 to ref=cardinality-1
	 */
	Set<?> orderedCoordinates();

}
