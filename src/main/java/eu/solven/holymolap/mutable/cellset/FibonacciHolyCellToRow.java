package eu.solven.holymolap.mutable.cellset;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionary;
import eu.solven.holymolap.primitives.ICompactable;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;

/**
 * This implementation of {@link IHolyCellToRow} will compress {@link IntList} into a long by packing coordinates into a
 * long. At some point, the underlying algorithm will not be able to accept additional {@link IntList} (as not packable
 * into a long). It would then be time to switch to a different algorithm.
 * 
 * This implementation trades CPU to gain RAM.
 * 
 * @author Benoit Lacelle
 *
 */
public class FibonacciHolyCellToRow implements IHolyCellToRow, ICompactable {
	private static final Logger LOGGER = LoggerFactory.getLogger(FibonacciHolyCellToRow.class);
	private static final AtomicInteger BUFFER_SIZE = new AtomicInteger(1);

	final Object2IntMap<ByteList> underlying;

	final ByteIntegerCODEC byteIntegerCodec;

	public FibonacciHolyCellToRow(Object2IntMap<ByteList> underlying, ByteIntegerCODEC byteIntegerCodec) {
		this.underlying = underlying;

		if (underlying.defaultReturnValue() != IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + underlying.defaultReturnValue());
		}

		this.byteIntegerCodec = byteIntegerCodec;
	}

	public FibonacciHolyCellToRow(ByteIntegerCODEC byteIntegerCodec) {
		this(defaultUnderlying(), byteIntegerCodec);
	}

	public FibonacciHolyCellToRow() {
		// We may submit arrays with -1 for NO_COORDINATES
		this(defaultUnderlying(), defaultCodec());
	}

	private static ByteIntegerCODEC defaultCodec() {
		return new FibonacciEncodingCodec(-1);
	}

	private static Object2IntMap<ByteList> defaultUnderlying() {
		Object2IntOpenHashMap<ByteList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	@Deprecated
	public static int getBufferSize() {
		return BUFFER_SIZE.get();
	}

	@Deprecated
	public static void resetBufferSize() {
		BUFFER_SIZE.set(1);
	}

	@Override
	public int getRow(IntList coordinates) {
		ByteList compressedCoordinates = compressToBytes(coordinates, false);
		return underlying.getInt(compressedCoordinates);
	}

	@Override
	public int registerRow(IntList coordinates) {
		int newIndex = underlying.size();

		ByteList compressedCoordinates = compressToBytes(coordinates, true);

		int previousValue = underlying.putIfAbsent(compressedCoordinates, newIndex);

		if (previousValue == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			// We mapped a value
			return newIndex;
		} else {
			// It was already mapped
			return previousValue;
		}
	}

	@Override
	public int size() {
		return underlying.size();
	}

	protected ByteList compressToBytes(IntList coordinates, boolean willBeStored) {
		assert coordinates.isEmpty() || coordinates.getInt(coordinates.size() - 1) != -1;

		byte[] bytes;
		IntWrapper outputPosition;

		while (true) {
			outputPosition = new IntWrapper();
			bytes = new byte[BUFFER_SIZE.get()];
			try {
				byteIntegerCodec.compress(coordinates
						.toIntArray(), new IntWrapper(), coordinates.size(), bytes, outputPosition);
				break;
			} catch (ArrayIndexOutOfBoundsException e) {
				LOGGER.debug("The buffer was too small (size={})", bytes.length, e);
				// JVM has same footprint for arrays from 1+8*N to 8*(N+1)
				int newSize = BUFFER_SIZE.addAndGet(8);
				LOGGER.info("We increased the buffer size to {}", newSize);
			}
		}

		if (willBeStored) {
			if (bytes.length > outputPosition.get()) {
				// We buffered too many bytes:
				bytes = Arrays.copyOfRange(bytes, 0, outputPosition.get());
			}
			return ByteArrayList.wrap(bytes);
		} else {
			// No need to minimize the array footprint as it will not be stored
			return ByteArrayList.wrap(bytes, outputPosition.get());
		}
	}

	@Override
	public void trim() {
		if (underlying instanceof Object2IntOpenHashMap<?>) {
			((Object2IntOpenHashMap<?>) underlying).trim();
		}
	}
}
