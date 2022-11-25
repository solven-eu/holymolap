package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * ENbaling mapping a cell coordinate (as an IntList) to a cellIndex (as an int).
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCellToRow {

	int getRow(IntList coordinates);

	int registerRow(IntList coordinates);

	int size();

}
