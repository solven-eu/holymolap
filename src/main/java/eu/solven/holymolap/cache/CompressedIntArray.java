package eu.solven.holymolap.cache;

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.tools.IHasMemoryFootprint;
import me.lemire.integercompression.IntCompressor;

public class CompressedIntArray implements IHasMemoryFootprint {
	private static final Logger LOGGER = LoggerFactory.getLogger(CompressedIntArray.class);

	final int[] compressed;

	// Move into ThreadLocal?
	transient IntCompressor iic = new IntCompressor();

	// https://stackoverflow.com/questions/299659/whats-the-difference-between-softreference-and-weakreference-in-java
	transient AtomicReference<SoftReference<int[]>> refSoftRef = new AtomicReference<>();

	public CompressedIntArray() {
		this.compressed = new int[0];
	}

	public CompressedIntArray(int[] rowToIndex) {
		this.compressed = iic.compress(rowToIndex);
		LOGGER.debug("Compressed from {} to {}", rowToIndex.length, compressed.length);
	}

	@Override
	public long getSizeInBytes() {
		return compressed.length * 4;
	}

	private int[] uncompress() {
		int[] rowToCoordinates;
		synchronized (this) {
			rowToCoordinates = iic.uncompress(compressed);
		}
		return rowToCoordinates;
	}

	public int[] getIntArray() {
		int[] uncompressed;

		SoftReference<int[]> softRef = refSoftRef.get();

		if (softRef == null) {
			uncompressed = null;
		} else {
			// Get the value from the softRef. May be null if GC'd
			uncompressed = softRef.get();
		}

		if (uncompressed == null) {
			uncompressed = uncompress();

			// Keep in-memory for re-use
			softRef = new SoftReference<int[]>(uncompressed);

			refSoftRef.compareAndSet(null, softRef);
		}

		return uncompressed;
	}
}
