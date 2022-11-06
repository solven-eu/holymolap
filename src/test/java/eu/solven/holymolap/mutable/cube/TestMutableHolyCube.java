package eu.solven.holymolap.mutable.cube;

import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.measures.definition.EmptyHolyMeasureTableDefinition;

public class TestMutableHolyCube {
	@Test
	public void testInference_onlyNew() {
		MutableHolyCube cube = new MutableHolyCube(new EmptyHolyMeasureTableDefinition());

		int[] inference = cube.computeInference(Arrays.asList("k2"), Arrays.asList("k1", "k2"));
		Assertions.assertThat(inference).hasSize(1).contains(1);
	}

	@Test
	public void testInference_missing() {
		MutableHolyCube cube = new MutableHolyCube(new EmptyHolyMeasureTableDefinition());

		Assertions.assertThat(cube.computeInference(Arrays.asList("k1", "k2"), Arrays.asList("k2")))
				.hasSize(2)
				.containsExactly(-1, 0);
	}
}
