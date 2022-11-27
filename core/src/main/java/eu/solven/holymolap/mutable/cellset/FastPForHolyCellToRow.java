package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import me.lemire.integercompression.FastPFOR128;

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
@Deprecated(since = "FastPFOR128 is not compatible with AIntegerCodecHolyCellToRow", forRemoval = true)
public class FastPForHolyCellToRow extends AIntegerCodecHolyCellToRow {
	public FastPForHolyCellToRow(Object2IntMap<IntList> underlying) {
		super(new FastPFOR128(), underlying);
	}

	public FastPForHolyCellToRow() {
		super(new FastPFOR128(), defaultUnderlying());
	}

}
