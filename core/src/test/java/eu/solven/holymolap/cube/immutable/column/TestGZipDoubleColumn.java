package eu.solven.holymolap.cube.immutable.column;

import eu.solven.holymolap.compression.doubles.GZipDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestGZipDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return GZipDoubleList.class;
	}

	@Override
	protected long expectedHeapConsuptionMin_lowDistinct() {
		return 200;
	}

	@Override
	protected long expectedHeapConsuptionMax_lowDistinct() {
		return 300;
	}

	@Override
	protected long expectedHeapConsuptionMin_aroundOne() {
		return 7_500;
	}

	@Override
	protected long expectedHeapConsuptionMax_aroundOne() {
		return 8_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_wereFloats() {
		return 4_500;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 5_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_positiveInts() {
		return 2_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 2_500;
	}
}