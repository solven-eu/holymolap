package eu.solven.holymolap.mutable.cellset;

import org.junit.runner.RunWith;
import org.quickperf.junit4.QuickPerfJUnitRunner;

@RunWith(QuickPerfJUnitRunner.class)
public class TestObject2IntHolyCellToRow extends ATestHolyCellToRow {

	@Override
	protected IReadableHolyCellToRow makeCellToRow() {
		return new Object2IntHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 123_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 125_000L;
	}
}
