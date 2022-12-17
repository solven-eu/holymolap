package eu.solven.holymolap.mutable.cellset;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Enables mapping a cell coordinate (as an IntList) to a cellIndex (as an int).
 * 
 * Given 2 equals coordinates, it is possible but not guaranteed they will be mapped to the same cellIndex.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAppendableHolyCellToRow {

	/**
	 * 
	 * @param coordinates
	 * @return if a new coordinate, a negative int (<code>(-(<i>cellIndex</i>) - 1)</code>, similarly to
	 *         {@link Arrays#binarySearch}). If positive, this returns a cellIndex for a previously encountered but
	 *         equals coordinate. It is up to the implementation to guarantee (or not) a bijection
	 *         coordinates<->cellIndex
	 */
	int getMayAppendRow(IntList coordinates);

	int size();

}
