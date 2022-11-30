package eu.solven.holymolap.cache;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IMayCache;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * Helps building data-structure which relies on a compressed one, and on-the-fly uncompressed cached through on a
 * {@link SoftReference}
 * 
 * @author Benoit Lacelle
 *
 * @param <T>
 */
public abstract class AHasSoftRefCache<U, C> implements IHasMemoryFootprint, IMayCache {
	private static final Logger LOGGER = LoggerFactory.getLogger(AHasSoftRefCache.class);

	final C compressed;

	// https://stackoverflow.com/questions/299659/whats-the-difference-between-softreference-and-weakreference-in-java
	transient AtomicReference<SoftReference<U>> refSoftRef = new AtomicReference<>();

	public AHasSoftRefCache(C compressed) {
		this.compressed = compressed;
	}

	public AHasSoftRefCache(U uncompressed, C compressed) {
		this.compressed = compressed;

		refSoftRef.set(makeReference(uncompressed));
	}

	protected SoftReference<U> makeReference(U uncompressed) {
		return new SoftReference<U>(uncompressed);
	}

	protected abstract long getSizeInBytesCompressed(C structure);

	protected abstract long getSizeInBytesUncompressed(U structure);

	@Override
	public long getSizeInBytes() {
		return getSizeInBytesCompressed(compressed);
	}

	@Override
	public void invalidateCache() {
		refSoftRef.set(null);
	}

	/**
	 * 
	 * @return the uncompressed from the compressed. This is slow as it does not rely on the cache.
	 */
	protected abstract U uncompress();

	protected C getCompressed() {
		return compressed;
	}

	public U getUncompressed() {
		U uncompressed;

		SoftReference<U> softRef = refSoftRef.get();

		if (softRef == null) {
			uncompressed = null;
		} else {
			// Get the value from the softRef. May be null if GC'd
			uncompressed = softRef.get();
		}

		if (uncompressed == null) {
			uncompressed = uncompress();

			LOGGER.debug("We uncompressed from footprint={} to footprint={}",
					getSizeInBytesCompressed(getCompressed()),
					getSizeInBytesUncompressed(uncompressed));

			// Keep in-memory for re-use
			softRef = makeReference(uncompressed);

			refSoftRef.compareAndSet(null, softRef);
		}

		return uncompressed;
	}
}
