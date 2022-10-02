package eu.solven.holymolap.mutable.axis;

public interface IMutableAxisSmallColumn {

	IMutableAxisSmallDictionarySink getCoordinateToIndex();

	int[] getRowToIndex();

	void appendCoordinate(Object coordinate);

	void appendCoordinateIndex(int coordinateRef);

	/**
	 * 
	 * @return The number of rows in given column.
	 */
	long getRows();

	/**
	 * An issue, may be one tried to append an incompatible type (a String into a Double column).
	 * 
	 * @return the number of rows having encountered an issue.
	 */
	long getBrokenRows();

}
