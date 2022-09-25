package eu.solven.holymolap.index;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.cube.immutable.AxisCoordinatesDictionary;
import eu.solven.holymolap.cube.immutable.IAxisCoordinatesDictionary;
import eu.solven.holymolap.cube.immutable.ImmutableAxisColumn;
import eu.solven.holymolap.cube.mutable.MutableAxisColumn;
import eu.solven.holymolap.cube.table.HolyDictionarizedTable;

public class TestHolyDictionarizedTable {

	final MutableAxisColumn mutableColumn1 = new MutableAxisColumn();
	final MutableAxisColumn mutableColumn2 = new MutableAxisColumn();
	{
		mutableColumn1.appendCoordinate("row0");
		mutableColumn1.appendCoordinate("row1");
		mutableColumn1.appendCoordinate("row0");
		mutableColumn1.appendCoordinate("row2");

		mutableColumn2.appendCoordinate("rowa");
		mutableColumn2.appendCoordinate("rowa");
		mutableColumn2.appendCoordinate("rowb");
		mutableColumn2.appendCoordinate("rowb");
	}

	@Test
	public void testScan_1Axes() {
		IAxisCoordinatesDictionary immutableDictionary =
				new AxisCoordinatesDictionary(mutableColumn1.getCoordinateToIndex().orderedCoordinates());

		ImmutableAxisColumn immutableColumn = new ImmutableAxisColumn(immutableDictionary, mutableColumn1);
		HolyDictionarizedTable table = new HolyDictionarizedTable(4, Arrays.asList(immutableColumn));

		{
			// row0
			Assertions.assertThat(table.getCellCoordinateRef(0, 0)).isEqualTo(0);
			// row1
			Assertions.assertThat(table.getCellCoordinateRef(1, 0)).isEqualTo(1);
			// row0
			Assertions.assertThat(table.getCellCoordinateRef(2, 0)).isEqualTo(0);
			// row2
			Assertions.assertThat(table.getCellCoordinateRef(3, 0)).isEqualTo(2);
		}

		// row0 on cells 0 and 2
		Assertions.assertThat(table.getCoordinateToRows(0, 0).toArray()).containsExactly(0, 2);
		// row1 on cells 1
		Assertions.assertThat(table.getCoordinateToRows(0, 1).toArray()).containsExactly(1);
		// row2 on cells 3
		Assertions.assertThat(table.getCoordinateToRows(0, 2).toArray()).containsExactly(3);
	}

	@Test
	public void testScan_2Axes() {
		IAxisCoordinatesDictionary immutableDictionary1 =
				new AxisCoordinatesDictionary(mutableColumn1.getCoordinateToIndex().orderedCoordinates());
		IAxisCoordinatesDictionary immutableDictionary2 =
				new AxisCoordinatesDictionary(mutableColumn2.getCoordinateToIndex().orderedCoordinates());

		ImmutableAxisColumn immutableColumn1 = new ImmutableAxisColumn(immutableDictionary1, mutableColumn1);
		ImmutableAxisColumn immutableColumn2 = new ImmutableAxisColumn(immutableDictionary2, mutableColumn2);
		HolyDictionarizedTable table = new HolyDictionarizedTable(4, Arrays.asList(immutableColumn1, immutableColumn2));

		{
			// row0
			Assertions.assertThat(table.getCellCoordinateRef(0, 0)).isEqualTo(0);
			Assertions.assertThat(table.getCellCoordinateRef(0, 1)).isEqualTo(0);
			// row1
			Assertions.assertThat(table.getCellCoordinateRef(1, 0)).isEqualTo(1);
			Assertions.assertThat(table.getCellCoordinateRef(1, 1)).isEqualTo(0);
			// row0
			Assertions.assertThat(table.getCellCoordinateRef(2, 0)).isEqualTo(0);
			Assertions.assertThat(table.getCellCoordinateRef(2, 1)).isEqualTo(1);
			// row2
			Assertions.assertThat(table.getCellCoordinateRef(3, 0)).isEqualTo(2);
			Assertions.assertThat(table.getCellCoordinateRef(3, 1)).isEqualTo(1);
		}

		// row0 on cells 0 and 2
		Assertions.assertThat(table.getCoordinateToRows(0, 0).toArray()).containsExactly(0, 2);
		// row1 on cells 1
		Assertions.assertThat(table.getCoordinateToRows(0, 1).toArray()).containsExactly(1);
		// row2 on cells 3
		Assertions.assertThat(table.getCoordinateToRows(0, 2).toArray()).containsExactly(3);

		// rowa on cells 0 and 1
		Assertions.assertThat(table.getCoordinateToRows(1, 0).toArray()).containsExactly(0, 1);
		// rowb on cells 2 and 3
		Assertions.assertThat(table.getCoordinateToRows(1, 1).toArray()).containsExactly(2, 3);
	}
}
