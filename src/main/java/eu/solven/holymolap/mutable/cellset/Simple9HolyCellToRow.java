package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import me.lemire.integercompression.Simple9;
import me.lemire.integercompression.VariableByte;

/**
 * A {@link IHolyCellToRow} compressing coordinates with {@link VariableByte}
 * 
 * @author Benoit Lacelle
 *
 */
public class Simple9HolyCellToRow extends AIntegerCodecHolyCellToRow {
	public Simple9HolyCellToRow(Object2IntMap<IntList> underlying) {
		super(new Simple9(), underlying);
	}

	public Simple9HolyCellToRow() {
		super(new Simple9(), defaultUnderlying());
	}
}
