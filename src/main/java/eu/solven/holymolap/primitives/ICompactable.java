package eu.solven.holymolap.primitives;

@Deprecated(since = "beta")
public interface ICompactable {

	/**
	 * Rehashes the map, making the table as small as possible.
	 *
	 * <p>
	 * This method rehashes the table to the smallest size satisfying the load factor. It can be used when the set will
	 * not be changed anymore, so to optimize access speed and size.
	 *
	 * <p>
	 * If the table size is already the minimum possible, this method does nothing.
	 */
	@Deprecated(since = "beta")
	void trim();

}
