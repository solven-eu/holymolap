package eu.solven.holymolap.mutable.axis;

/**
 * Bijection between coordinates and indexes (from 0, to the cardinality of the dictionary).
 * 
 * It accepts new coordinates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableAxisSmallIntDictionary extends IMutableAxisSmallDictionary {

	@Deprecated
	@Override
	default int getIndexMayMiss(Object coordinate) {
		if (!(coordinate instanceof Number)) {
			throw new IllegalArgumentException("Expected a Number");
		}
		return getIndexMayMiss(((Number) coordinate).intValue());
	}

	@Deprecated
	@Override
	default int getIndexMayAppend(Object coordinate) {
		if (!(coordinate instanceof Number)) {
			throw new IllegalArgumentException("Expected a Number");
		}
		return getIndexMayAppend(((Number) coordinate).intValue());
	}

	int getIntIndexMayMiss(int coordinate);

	/**
	 * 
	 * @param coordinate
	 * @return the coordinate index, being the new cardinality in case of insertion.
	 */
	int getIntIndexMayAppend(int coordinate);
}
