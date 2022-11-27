package eu.solven.holymolap.cube.immutable.column;

import eu.solven.holymolap.compression.doubles.DictionaryDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestDictionaryDoubleList extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return DictionaryDoubleList.class;
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
		return 12_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_aroundOne() {
		return 13_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_wereFloats() {
		return 12_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_wereFloats() {
		return 13_000;
	}

	@Override
	protected long expectedHeapConsuptionMin_positiveInts() {
		return 12_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 13_000;
	}
}
