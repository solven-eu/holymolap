package eu.solven.holymolap.cube.immutable;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import eu.solven.holymolap.immutable.column.ImmutableDoubleAggregatesColumn;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

public class TestImmutableDoubleAggregatesColumn {
	@Test
	public void testIterateOutOfUpperBound() {
		ImmutableDoubleAggregatesColumn column = new ImmutableDoubleAggregatesColumn(new DoubleArrayList(), 1.23D);

		// Read 3 rows with the column in empty
		LongList rowIndexes = new LongArrayList(new long[] { 0L, 1L, 5L });

		DoubleIterator doubleIterator = column.mapToDouble(rowIndexes.iterator());

		// row 0L
		Assertions.assertThat(doubleIterator.hasNext()).isTrue();
		Assertions.assertThat(doubleIterator.nextDouble()).isEqualTo(1.23D);

		// row 1L
		Assertions.assertThat(doubleIterator.hasNext()).isTrue();
		Assertions.assertThat(doubleIterator.nextDouble()).isEqualTo(1.23D);

		// row 5L
		Assertions.assertThat(doubleIterator.hasNext()).isTrue();
		Assertions.assertThat(doubleIterator.nextDouble()).isEqualTo(1.23D);

		Assertions.assertThat(doubleIterator.hasNext()).isFalse();
	}
}
