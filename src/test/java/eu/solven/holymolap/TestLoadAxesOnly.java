package eu.solven.holymolap;

import java.util.Arrays;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasureTableDefinition;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.sink.record.HolyCubeRecord;

public class TestLoadAxesOnly implements IHolyMapDataTestConstants {

	final IHolyMeasuresDefinition definitions = HolyMeasureTableDefinition.withCountStar(Collections.emptyList());
	final IHolyCubeSink sink = new HolyCubeSink(definitions);

	@Test
	public void testAddOneColumn_SecondOtherColumn() {
		// Check firstKey is before secondKey
		Assertions.assertThat(FIRST_KEY.compareTo(SECOND_KEY)).isNegative();

		IHolyCube cube = sink
				.sink(new HolyCubeRecord(new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE }),
						EmptyHolyRecord.INSTANCE))
				.sink(new HolyCubeRecord(new FastEntry(Arrays.asList(SECOND_KEY), new Object[] { SECOND_VALUE }),
						EmptyHolyRecord.INSTANCE))
				.closeToHolyCube();

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().axes()).containsExactly(FIRST_KEY, SECOND_KEY);

		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().getAxisIndex(FIRST_KEY)).isEqualTo(0);
		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().getAxisIndex(SECOND_KEY)).isEqualTo(1);

		Assertions.assertThat(cube.getCellSet().getTable().getCellCoordinates(0, 0, 1)).containsExactly(0, -1);
		Assertions.assertThat(cube.getCellSet().getTable().getCellCoordinates(1, 0, 1)).containsExactly(-1, 0);
	}

	@Test
	public void testAddOneColumn_SecondOtherColumn_reverseOrder() {
		// Check firstKey is before secondKey
		Assertions.assertThat(FIRST_KEY.compareTo(SECOND_KEY)).isNegative();

		IHolyCube cube = sink
				.sink(new HolyCubeRecord(new FastEntry(Arrays.asList(SECOND_KEY), new Object[] { SECOND_VALUE }),
						EmptyHolyRecord.INSTANCE))
				.sink(new HolyCubeRecord(new FastEntry(Arrays.asList(FIRST_KEY), new Object[] { FIRST_VALUE }),
						EmptyHolyRecord.INSTANCE))
				.closeToHolyCube();

		Assertions.assertThat(cube.getNbRows()).isEqualTo(2);
		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().axes()).containsExactly(FIRST_KEY, SECOND_KEY);

		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().getAxisIndex(FIRST_KEY)).isEqualTo(0);
		Assertions.assertThat(cube.getCellSet().getAxesWithCoordinates().getAxisIndex(SECOND_KEY)).isEqualTo(1);

		Assertions.assertThat(cube.getCellSet().getTable().getCellCoordinates(0, 0, 1)).containsExactly(-1, 0);
		Assertions.assertThat(cube.getCellSet().getTable().getCellCoordinates(1, 0, 1)).containsExactly(0, -1);
	}
}
