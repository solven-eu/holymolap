package eu.solven.holymolap.sink.record;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestFastEntry {
	@Test
	public void testObjectIntAndDouble() {
		FastEntry entry = new FastEntry(Arrays.asList("o", "i", "d"),
				new Object[] { "someString" },
				new long[] { 123 },
				new double[] { 456.789D });

		entry.accept((axisIndex, coordinate) -> {
			if (axisIndex == 0) {
				Assertions.assertThat(coordinate).isEqualTo("someString");
			} else if (axisIndex == 1) {
				Assertions.assertThat(coordinate).isEqualTo(123L);
			} else if (axisIndex == 2) {
				Assertions.assertThat(coordinate).isEqualTo(456.789D);
			} else {
				Assertions.fail("Should not happen");
			}
		});
	}

	@Test
	public void testObjectAndDouble() {
		FastEntry entry = new FastEntry(Arrays.asList("o", "d"),
				new Object[] { "someString" },
				new long[] {},
				new double[] { 456.789D });

		entry.accept((axisIndex, coordinate) -> {
			if (axisIndex == 0) {
				Assertions.assertThat(coordinate).isEqualTo("someString");
			} else if (axisIndex == 1) {
				Assertions.assertThat(coordinate).isEqualTo(456.789D);
			} else {
				Assertions.fail("Should not happen");
			}
		});
	}
}
