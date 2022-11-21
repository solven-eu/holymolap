package eu.solven.holymolap.mutable.cellset;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.openjdk.jol.info.GraphLayout;
import org.quickperf.junit4.QuickPerfJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(QuickPerfJUnitRunner.class)
public class TestVariableByteIntHolyCellToRow extends ATestHolyCellToRow {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestVariableByteIntHolyCellToRow.class);

	@Override
	protected IHolyCellToRow makeCellToRow() {
		return new VariableByteHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 83_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 86_000L;
	}

	// This demonstrates byte[] of length 1+N*8->(N+1)*8 has the same footprint
	@Test
	public void testMemoryByteArrays() {
		for (int i = 0; i < 32; i++) {
			LOGGER.info(i + ": " + GraphLayout.parseInstance(new byte[i]).totalSize());
		}
	}

	// This demonstrates int[] of length 1+N*2->(N+1)*2 has the same footprint
	@Test
	public void testMemoryIntArrays() {
		for (int i = 0; i < 32; i++) {
			LOGGER.info(i + ": " + GraphLayout.parseInstance(new int[i]).totalSize());
		}
	}
}
