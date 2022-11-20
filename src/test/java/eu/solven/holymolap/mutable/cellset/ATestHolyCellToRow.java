package eu.solven.holymolap.mutable.cellset;

import java.util.Random;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openjdk.jol.info.GraphLayout;
import org.quickperf.junit4.QuickPerfJUnitRunner;
import org.quickperf.jvm.annotations.MeasureHeapAllocation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

@RunWith(QuickPerfJUnitRunner.class)
public abstract class ATestHolyCellToRow {

	private void standardLoad(IHolyCellToRow cellToRow) {
		Random r = new Random(123L);

		for (int i = 0; i < 1024; i++) {
			int[] coordinates = IntStream.range(0, 16).map(c -> r.nextInt(0, 1 + c * c * c)).toArray();
			cellToRow.registerRow(IntArrayList.wrap(coordinates));
		}
	}

	protected abstract IHolyCellToRow makeCellToRow();

	protected abstract long expectedHeapConsuptionMin();

	protected abstract long expectedHeapConsuptionMax();

	@Ignore("https://github.com/quick-perf/quickperf/issues/191")
	@MeasureHeapAllocation
	// @ExpectMaxHeapAllocation(value = 440, unit = AllocationUnit.BYTE)
	@Test
	public void testHeapAllocation() {
		IHolyCellToRow cellToRow = makeCellToRow();

		standardLoad(cellToRow);
	}

	@Test
	public void testHeaplayout() {
		IHolyCellToRow cellToRow = makeCellToRow();

		standardLoad(cellToRow);

		Assertions.assertThat(GraphLayout.parseInstance(cellToRow).totalSize())
				.isBetween(expectedHeapConsuptionMin(), expectedHeapConsuptionMax());
	}

	private void consistencyChecks(IHolyCellToRow cellToRow, IntArrayList array) {
		Assertions.assertThat(cellToRow.getRow(array)).isEqualTo(-1);

		Assertions.assertThat(cellToRow.registerRow(IntArrayList.of(-1, 0))).isEqualTo(0);
		Assertions.assertThat(cellToRow.registerRow(IntArrayList.of(-1, 0))).isEqualTo(0);

		Assertions.assertThat(cellToRow.getRow(IntArrayList.of(-1, 0))).isEqualTo(0);
	}

	@Test
	public void testHandleArray() {
		IHolyCellToRow cellToRow = makeCellToRow();

		IntArrayList array = IntArrayList.of(123);
		consistencyChecks(cellToRow, array);
	}

	@Test
	public void testHandleArrayWithMissingCoordinates() {
		IHolyCellToRow cellToRow = makeCellToRow();

		consistencyChecks(cellToRow, IntArrayList.of(-1, 123));
	}
}
