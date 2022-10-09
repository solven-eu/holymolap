package eu.solven.holymolap.mutable.column;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.pepper.memory.IPepperMemoryConstants;

public class TestMutableDoubleAggregatesColumn {
	@Test
	public void testWriteNeutral() {
		MutableDoubleAggregatesColumn column = new MutableDoubleAggregatesColumn(IStandardOperators.SUM);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateDouble(Integer.MAX_VALUE / 2, 0D);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(sizeBefore);
	}

	@Test
	public void testWriteOne_first() {
		MutableDoubleAggregatesColumn column = new MutableDoubleAggregatesColumn(IStandardOperators.SUM);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateDouble(0, 1.2D);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(80L);
	}

	@Test
	public void testWriteOne_further() {
		MutableDoubleAggregatesColumn column = new MutableDoubleAggregatesColumn(IStandardOperators.SUM);

		long sizeBefore = column.getSizeInBytes();
		Assertions.assertThat(sizeBefore).isLessThanOrEqualTo(IPepperMemoryConstants.MB);

		column.aggregateDouble(123, 1.2D);

		Assertions.assertThat(column.getSizeInBytes()).isEqualTo(1304L);
	}
}
