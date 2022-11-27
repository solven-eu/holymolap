package eu.solven.holymolap.mutable.axis;

public interface IAppendOnlyIntList {

	void append(int coordinateIndex);

	long size();

	/**
	 * Copies elements of this list into the given array.
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
	void getElements(int from, int[] a, int offset, int length);

}
