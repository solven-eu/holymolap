package eu.solven.holymolap.mutable.column;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.pepper.memory.IPepperMemoryConstants;

public class TestMutableLongAggregatesColumn {
	@Test
	public void testWriteNeutral() {
		MutableLongAggregatesColumn column = new MutableLongAggregatesColumn(IStandardOperators.COUNT);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateLong(Integer.MAX_VALUE / 2, 0L);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(sizeBefore);
	}

	@Test
	public void testWriteOne_first() {
		MutableLongAggregatesColumn column = new MutableLongAggregatesColumn(IStandardOperators.COUNT);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateLong(0, 123L);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(80L);
	}

	@Test
	public void testWriteOne_further() {
		MutableLongAggregatesColumn column = new MutableLongAggregatesColumn(IStandardOperators.COUNT);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateLong(123, 132L);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(1304L);
	}
}
