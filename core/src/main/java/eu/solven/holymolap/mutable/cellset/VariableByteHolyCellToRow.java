package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import me.lemire.integercompression.VariableByte;

/**
 * A {@link IReadableHolyCellToRow} compressing coordinates with {@link VariableByte}
 * 
 * @author Benoit Lacelle
 *
 */
public class VariableByteHolyCellToRow extends FibonacciHolyCellToRow {

	public VariableByteHolyCellToRow(Object2IntMap<ByteList> underlying) {
		super(underlying, new VariableByte());
	}

	public VariableByteHolyCellToRow() {
		super(new VariableByte());
	}

}
