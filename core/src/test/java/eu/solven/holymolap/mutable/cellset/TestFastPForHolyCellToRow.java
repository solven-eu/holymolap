package eu.solven.holymolap.mutable.cellset;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.quickperf.junit4.QuickPerfJUnitRunner;

@Ignore("FastPForHolyCellToRow is not functional")
@RunWith(QuickPerfJUnitRunner.class)
public class TestFastPForHolyCellToRow extends ATestHolyCellToRow {

	@Override
	protected IReadableHolyCellToRow makeCellToRow() {
		return new FastPForHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 1_000_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 1_100_000L;
	}
}
