package eu.solven.holymolap.aggregate;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;

public class TestCoordinatesRefs {
	@Test
	public void testToString() {
		IHasAxesWithCoordinates axesWithCoordinates = Mockito.mock(IHasAxesWithCoordinates.class);
		Mockito.when(axesWithCoordinates.getAxes()).thenReturn(Arrays.asList("axis0", "axis1"));
		Mockito.when(axesWithCoordinates.dereferenceCoordinate(1, 123)).thenReturn("valueAt123");

		CoordinatesRefs coordinatesRef = new CoordinatesRefs(axesWithCoordinates, new int[] { 1 }, new long[] { 123 });

		Assertions.assertThat(coordinatesRef.toString()).isEqualTo("axis1->'valueAt123'");
	}
}
