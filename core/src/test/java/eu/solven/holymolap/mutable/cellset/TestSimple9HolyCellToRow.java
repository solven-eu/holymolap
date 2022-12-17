package eu.solven.holymolap.mutable.cellset;

import org.junit.runner.RunWith;
import org.quickperf.junit4.QuickPerfJUnitRunner;

@RunWith(QuickPerfJUnitRunner.class)
public class TestSimple9HolyCellToRow extends ATestHolyCellToRow {

	@Override
	protected IReadableHolyCellToRow makeCellToRow() {
		return new Simple9HolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 90_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 95_000L;
	}
}
