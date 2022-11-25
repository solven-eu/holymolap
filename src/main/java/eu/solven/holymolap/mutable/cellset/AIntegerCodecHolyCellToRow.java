package eu.solven.holymolap.mutable.cellset;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;

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
public class AIntegerCodecHolyCellToRow extends Object2IntHolyCellToRow {
	private static final Logger LOGGER = LoggerFactory.getLogger(AIntegerCodecHolyCellToRow.class);
	private static final AtomicInteger BUFFER_SIZE = new AtomicInteger(1);

	// We may submit arrays with -1 for NO_COORDINATES
	final IntegerCODEC integerCodec;

	public AIntegerCodecHolyCellToRow(IntegerCODEC integerCodec, Object2IntMap<IntList> underlying) {
		super(underlying);

		this.integerCodec = integerCodec;
	}

	public AIntegerCodecHolyCellToRow(IntegerCODEC integerCodec) {
		super(defaultUnderlying());

		this.integerCodec = integerCodec;
	}

	@Deprecated
	public static int getBufferSize() {
		return BUFFER_SIZE.get();
	}

	@Deprecated
	public static void resetBufferSize() {
		BUFFER_SIZE.set(1);
	}

	public IntegerCODEC getIntegerCodec() {
		return integerCodec;
	}

	@Override
	protected IntList compress(IntList coordinates, boolean willBeStored) {
		assert coordinates.isEmpty() || coordinates.getInt(coordinates.size() - 1) != -1;

		int[] ints;
		IntWrapper outputPosition;

		IntegerCODEC codec = getIntegerCodec();
		while (true) {
			outputPosition = new IntWrapper();
			ints = new int[BUFFER_SIZE.get()];
			try {
				IntWrapper inPosition = new IntWrapper();
				codec.compress(coordinates.toIntArray(), inPosition, coordinates.size(), ints, outputPosition);

				if (inPosition.get() != coordinates.size()) {
					// e.g. FastPFOR128 requires 128 input ints
					throw new IllegalArgumentException("We require a codec accepting any input size");
				}

				break;
			} catch (ArrayIndexOutOfBoundsException e) {
				// JVM footprint for in[] as the same for array from 1+2N to 2*(N+1)
				int newSize = BUFFER_SIZE.addAndGet(2);
				LOGGER.info("We increased the buffer size to {}", newSize);
			}
		}

		if (willBeStored) {
			if (ints.length > outputPosition.get()) {
				// We buffered too many bytes:
				ints = Arrays.copyOfRange(ints, 0, outputPosition.get());
			}
			return IntArrayList.wrap(ints);
		} else {
			// No need to minimize the array footprint as it will not be stored
			return IntArrayList.wrap(ints, outputPosition.get());
		}
	}
}
