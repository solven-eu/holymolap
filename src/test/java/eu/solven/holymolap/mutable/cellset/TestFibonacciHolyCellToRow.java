package eu.solven.holymolap.mutable.cellset;

import org.junit.runner.RunWith;
import org.quickperf.junit4.QuickPerfJUnitRunner;

@RunWith(QuickPerfJUnitRunner.class)
public class TestFibonacciHolyCellToRow extends ATestHolyCellToRow {

	@Override
	protected IHolyCellToRow makeCellToRow() {
		return new FibonacciHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 23_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 25_000L;
	}
}
