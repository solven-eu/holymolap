package eu.solven.holymolap.mutable.cellset;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class TestSwitchOnSizeCellToRow {
	@Test
	public void testOverflow() {
		SwitchOnSizeCellToRow cellToRow = new SwitchOnSizeCellToRow(2);

		// First insertion on smart
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(0))).isEqualTo(-1);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(0))).isEqualTo(0);

		// Second insertion on smart
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(1))).isEqualTo(-2);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(1))).isEqualTo(1);

		// First insertion on naive
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(2))).isEqualTo(-3);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(2))).isEqualTo(-4);

		// Second insertion on naive
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(3))).isEqualTo(-5);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(3))).isEqualTo(-6);

		// Full read
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(0))).isEqualTo(0);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(1))).isEqualTo(1);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(2))).isEqualTo(-7);
		Assertions.assertThat(cellToRow.getMayAppendRow(IntArrayList.of(3))).isEqualTo(-8);
	}
}
