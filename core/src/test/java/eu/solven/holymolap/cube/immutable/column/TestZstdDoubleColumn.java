package eu.solven.holymolap.cube.immutable.column;

import eu.solven.holymolap.compression.doubles.ZstdDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestZstdDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return ZstdDoubleList.class;
	}

	@Override
	protected long expectedHeapConsuptionMin_lowDistinct() {
		return 150_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_lowDistinct() {
		return 151_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_aroundOne() {
		return 158_00;
	}

	@Override
	protected long expectedHeapConsuptionMax_aroundOne() {
		return 159_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_wereFloats() {
		return 154_00;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 155_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_positiveInts() {
		return 152_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 153_000;
	}
}