package eu.solven.holymolap.mutable.cellset;

import java.util.concurrent.atomic.AtomicInteger;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * A naive {@link IReadableHolyCellToRow} implementation, covering any case which would not be covered by alternative
 * implementations (e.g. due to constraints related to compression)
 * 
 * @author Benoit Lacelle
 *
 */
public class NaiveHolyCellToRow implements IAppendableHolyCellToRow {
	final AtomicInteger nextCellIndex = new AtomicInteger();

	public NaiveHolyCellToRow(int nextCellIndex) {
		this.nextCellIndex.set(nextCellIndex);
	}

	public NaiveHolyCellToRow() {
		this(0);
	}

	@Override
	public int getMayAppendRow(IntList coordinates) {
		int newIndex = nextCellIndex.getAndIncrement();

		// We mapped a value
		return -newIndex - 1;
	}

	@Override
	public int size() {
		return nextCellIndex.get();
	}

}
