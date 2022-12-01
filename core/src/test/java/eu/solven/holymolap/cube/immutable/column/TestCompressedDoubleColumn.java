package eu.solven.holymolap.cube.immutable.column;

import eu.solven.holymolap.compression.doubles.DoubleAsFourBytesDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestCompressedDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return DoubleAsFourBytesDoubleList.class;
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

	@Override
	protected long expectedHeapConsuptionMin_positiveInts() {
		return 4_00;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 4_500;
	}
}
