package eu.solven.holymolap.cube.immutable.column;

import eu.solven.holymolap.compression.doubles.DynamicSchemeDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public class TestDynamicSchemeImmutableDoubleColumn extends ATestCompressedDoubleList {

	@Override
	public Class<? extends DoubleList> getClazz() {
		return DynamicSchemeDoubleList.class;
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
		return 1_000;
	}

	@Override
	protected long expectedHeapConsuptionMax_positiveInts() {
		return 1_500;
	}
}
