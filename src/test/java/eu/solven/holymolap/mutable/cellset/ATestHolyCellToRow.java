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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.primitives.ICompactable;
import it.unimi.dsi.fastutil.ints.IntArrayList;

@RunWith(QuickPerfJUnitRunner.class)
public abstract class ATestHolyCellToRow {
	private static final Logger LOGGER = LoggerFactory.getLogger(ATestHolyCellToRow.class);

	private void standardLoad(IHolyCellToRow cellToRow) {
		Random r = new Random(123L);

		for (int cellIndex = 0; cellIndex < 1024; cellIndex++) {
			int[] coordinates = IntStream.range(0, 16).map(c -> r.nextInt(0, 1 + (int) Math.pow(c, 3))).toArray();
			cellToRow.registerRow(IntArrayList.wrap(coordinates));
		}

		if (cellToRow instanceof ICompactable) {
			((ICompactable) cellToRow).trim();
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
	public void testHeapLayout() {
		IHolyCellToRow cellToRow = makeCellToRow();

		standardLoad(cellToRow);

		GraphLayout graph = GraphLayout.parseInstance(cellToRow);
		Assertions.assertThat(graph.totalSize()).isBetween(expectedHeapConsuptionMin(), expectedHeapConsuptionMax());

		LOGGER.info("Graph.footprint: {}", graph.toFootprint());
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
