package eu.solven.holymolap.cube.immutable.column;

import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestCompressedDoubleColumn_v2 extends ATestCompressedDoubleList {
	@Test
	public void testReArrange_noChange() {
		int[] lessFrequentFirst = IntStream.range(0, 64).toArray();
		Assertions.assertThat(CompressedDoubleColumn_v2.reArrange(lessFrequentFirst, 1L)).isEqualTo(1L);
	}

	@Test
	public void testReArrange_reverse() {
		int[] lessFrequentFirst = IntStream.range(0, 64).map(i -> 63 - i).toArray();
		Assertions.assertThat(CompressedDoubleColumn_v2.reArrange(lessFrequentFirst, 1L)).isEqualTo(Long.MIN_VALUE);
	}

	@Test
	public void testReArrange_arrange1D() {
		// 0011111111110000000000000000000000000000000000000000000000000000
		long l = Double.doubleToLongBits(1D);

		int[] lessFrequentFirst = IntStream
				.concat(IntStream.range(0, 2), IntStream.concat(IntStream.range(12, 64), IntStream.range(2, 12)))
				.toArray();
		// Should be turned into 0000000000000000000000000000000000000000000000000000001111111111
		Assertions.assertThat(CompressedDoubleColumn_v2.reArrange(lessFrequentFirst, l)).isEqualTo((1 << 10) - 1);
	}

	@Override
	public Class<? extends DoubleList> getClazz() {
		return CompressedDoubleColumn_v2.class;
	}

	private void checkReadWrite(double... inputs) {
		DoubleList c = new CompressedDoubleColumn_v2(inputs);

		for (int i = 0; i < inputs.length; i++) {
			double input = inputs[i];
			if (Double.isNaN(input)) {
				Assertions.assertThat(c.getDouble(i)).isNaN();
			} else {
				Assertions.assertThat(c.getDouble(i)).isEqualTo(input);
			}
		}
	}

	@Override
	protected long expectedHeapConsuptionMin_lowDistinct() {
		return 4_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_lowDistinct() {
		return 4_500;
	}

	@Override
	protected long expectedHeapConsuptionMin_aroundOne() {
		return 8_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_aroundOne() {
		return 8_500;
	}

	@Override
	protected long expectedHeapConsuptionMin_wereFloats() {
		return 4_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 4_500;
	}

	@Override
	protected long expectedHeapConsuptionMin_positiveInts() {
		return 4_00;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 4_500;
	}

	@Test
	public void testBasics_singleValue() {
		checkReadWrite(0D);
		checkReadWrite(1D);
		checkReadWrite(-1D);

		checkReadWrite(132.456D);
		checkReadWrite(-456.123D);

		checkReadWrite(132456789.456789132465789123456789e132D);
		checkReadWrite(-132456789.456789132465789123456789e132D);

		checkReadWrite(Double.NEGATIVE_INFINITY);
		checkReadWrite(Double.POSITIVE_INFINITY);
		checkReadWrite(Double.NaN);
		checkReadWrite(Double.MIN_NORMAL);
		checkReadWrite(Double.MIN_VALUE);
		checkReadWrite(Double.MAX_VALUE);
	}

	@Test
	public void testBasics_twoValues() {
		checkReadWrite(0D, 1D);
		checkReadWrite(1D, -1D);
		checkReadWrite(-1D, 1D);

		checkReadWrite(132.456D, -456.123D);
		checkReadWrite(-456.123D, 132.456D);

		checkReadWrite(132456789.456789132465789123456789e132D, -132456789.456789132465789123456789e132D);

		checkReadWrite(1D, Double.NEGATIVE_INFINITY);
		checkReadWrite(Double.POSITIVE_INFINITY, 1D);
		checkReadWrite(1D, Double.NaN);
		checkReadWrite(Double.MIN_NORMAL, 1D);
		checkReadWrite(1D, Double.MIN_VALUE);
		checkReadWrite(Double.MAX_VALUE, 1D);
	}
}