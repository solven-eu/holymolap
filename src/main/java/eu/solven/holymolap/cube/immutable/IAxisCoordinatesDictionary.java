package eu.solven.holymolap.cube.immutable;

import java.util.Collection;
import java.util.Objects;

/**
 * Like VBA Dictionary, this maps {@link Long} to {@link Objects}, from 0 to the cardinality of the dictionary. No 2
 * indexes would maps to equals Objects.
 * 
 * A dummy implementation would rely on an array of distinct values. The index in the dictionary being the index of
 * given item in the array, while the array would not accept duplicates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAxisCoordinatesDictionary {
	int NOT_INDEXED = -1;
	Object NO_REFERENCE = new Object();

	/**
	 * 
	 * @param value
	 * @return the index for given coordinate, else NOT_INDEXED
	 */
	long getCoordinateRef(Object value);

	/**
	 * 
	 * @param value
	 * @return the index of given coordinate, indexing it if it is not yet indexed
	 */
	// long mapCoordinateIndex(Object value);

	Object getCoordinate(long coordinateRef);

	Collection<?> coordinates();
}
