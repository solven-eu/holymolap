package eu.solven.holymolap.cube.immutable.column;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import eu.solven.holymolap.compression.doubles.FcpDoubleColumn;
import it.unimi.dsi.fastutil.doubles.DoubleList;

@Ignore("This FCP implementation looks broken")
public class TestFcpDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return FcpDoubleColumn.class;
	}

	private void checkReadWrite(double... inputs) {
		DoubleList c = new FcpDoubleColumn(inputs);

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