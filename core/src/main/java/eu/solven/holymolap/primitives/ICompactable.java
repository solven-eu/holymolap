package eu.solven.holymolap.primitives;

/**
 * This interface can be put on any data-structure which can be compacted. Compaction may take different forms: some
 * array-based may re-allocate a smaller array if the initial one was buffered too large. Others may compress (with any
 * compression-scheme), hence requiring CPU to later uncompress the data.
 * 
 * @author Benoit Lacelle
 *
 */
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
