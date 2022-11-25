package eu.solven.holymolap.cube.immutable.column;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestDoubleArray extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return DoubleArrayList.class;
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
		return 8_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 8_500;
	}
}
