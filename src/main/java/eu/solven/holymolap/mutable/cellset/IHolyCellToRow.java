package eu.solven.holymolap.mutable.cellset;

import it.unimi.dsi.fastutil.ints.IntList;

public interface IHolyCellToRow {

	int getRow(IntList coordinates);

	int registerRow(IntList coordinates);

	int size();

}
