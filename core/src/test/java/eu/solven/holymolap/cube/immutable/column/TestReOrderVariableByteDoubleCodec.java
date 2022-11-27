package eu.solven.holymolap.cube.immutable.column;

import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.compression.doubles.ReOrderVariableByteDoubleCodec;

public class TestReOrderVariableByteDoubleCodec {
	@Test
	public void testReArrange_noChange() {
		int[] lessFrequentFirst = IntStream.range(0, 64).toArray();
		Assertions.assertThat(ReOrderVariableByteDoubleCodec.reArrange(lessFrequentFirst, 1L)).isEqualTo(1L);
	}

	@Test
	public void testReArrange_reverse() {
		int[] lessFrequentFirst = IntStream.range(0, 64).map(i -> 63 - i).toArray();
		Assertions.assertThat(ReOrderVariableByteDoubleCodec.reArrange(lessFrequentFirst, 1L))
				.isEqualTo(Long.MIN_VALUE);
	}

	@Test
	public void testReArrange_arrange1D() {
		// 0011111111110000000000000000000000000000000000000000000000000000
		long l = Double.doubleToLongBits(1D);

		int[] lessFrequentFirst = IntStream
				.concat(IntStream.range(0, 2), IntStream.concat(IntStream.range(12, 64), IntStream.range(2, 12)))
				.toArray();
		// Should be turned into 0000000000000000000000000000000000000000000000000000001111111111
		Assertions.assertThat(ReOrderVariableByteDoubleCodec.reArrange(lessFrequentFirst, l)).isEqualTo((1 << 10) - 1);
	}
}
