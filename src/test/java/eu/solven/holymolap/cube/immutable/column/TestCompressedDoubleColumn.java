package eu.solven.holymolap.cube.immutable.column;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestCompressedDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return CompressedDoubleColumn.class;
	}

	private void checkReadWrite(double... inputs) {
		CompressedDoubleColumn c = new CompressedDoubleColumn(inputs);

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
		return 8_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_lowDistinct() {
		return 8_500;
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
		return 6_400;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 6_500;
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
