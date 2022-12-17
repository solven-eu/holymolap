package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * A {@link IAppendableHolyCellToRow} which can be read with being appended.
 * 
 * There is no guarantee the same coordinate is always mapped to the same cellIndex.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IReadableHolyCellToRow extends IAppendableHolyCellToRow {

	int getRow(IntList coordinates);

}
