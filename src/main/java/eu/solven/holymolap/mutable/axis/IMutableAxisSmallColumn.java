package eu.solven.holymolap.mutable.axis;

public interface IMutableAxisSmallColumn {

	IMutableAxisSmallDictionarySink getCoordinateToRef();

	// default int[] getRowToIndex() {
	// return getRowToIndex(new int[0]);
	// }

	/**
	 * Copies (hopefully quickly) elements of this type-specific list into the given array.
	 *
	 * @param from
	 *            the start index (inclusive).
	 * @param a
	 *            the destination array.
	 * @param offset
	 *            the offset into the destination array where to store the first element copied.
	 * @param length
	 *            the number of elements to be copied.
	 */
	void getRowToIndex(int from, int a[], int offset, int length);

	void appendCoordinate(Object coordinate);

	void appendCoordinateRef(int coordinateRef);

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
