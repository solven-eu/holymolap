package eu.solven.holymolap.cache;

import me.lemire.integercompression.IntCompressor;

public class CompressedIntArray extends AHasSoftRefCache<int[], int[]> {
	// Move into ThreadLocal?
	transient IntCompressor iic = new IntCompressor();

	public CompressedIntArray() {
		this(new int[0]);
	}

	public CompressedIntArray(int[] uncompressed) {
		this(new IntCompressor(), uncompressed);
	}

	public CompressedIntArray(IntCompressor iic, int[] uncompressed) {
		super(iic.compress(uncompressed));

		this.iic = iic;
	}

	@Override
	protected long getSizeInBytesCompressed(int[] structure) {
		return structure.length * Integer.BYTES;
	}

	@Override
	protected long getSizeInBytesUncompressed(int[] structure) {
		return structure.length * Integer.BYTES;
	}

	@Override
	protected int[] uncompress() {
		int[] rowToCoordinates;
		synchronized (this) {
			rowToCoordinates = iic.uncompress(getCompressed());
		}
		return rowToCoordinates;
	}

	public int[] getIntArray() {
		return getUncompressed();
	}
}
