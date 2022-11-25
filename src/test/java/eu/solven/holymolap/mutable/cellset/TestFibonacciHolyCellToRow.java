package eu.solven.holymolap.mutable.cellset;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.quickperf.junit4.QuickPerfJUnitRunner;

import it.unimi.dsi.fastutil.ints.IntArrayList;

@RunWith(QuickPerfJUnitRunner.class)
public class TestFibonacciHolyCellToRow extends ATestHolyCellToRow {

	@Override
	protected IHolyCellToRow makeCellToRow() {
		return new FibonacciHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 80_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 85_000L;
	}

	@Test
	public void testBufferSize() {
		// Initial size is 1
		FibonacciHolyCellToRow.resetBufferSize();
		Assertions.assertThat(FibonacciHolyCellToRow.getBufferSize()).isEqualTo(0);

		IHolyCellToRow cellToRow = makeCellToRow();

		// Even a .getRow increased the bufferSize
		cellToRow.getRow(IntArrayList.of(5 - 2, 1 - 2, 10 - 2, 143 - 2));
		// The bufferSize is increased 8 by 8
		Assertions.assertThat(FibonacciHolyCellToRow.getBufferSize()).isEqualTo(8);
	}
}
