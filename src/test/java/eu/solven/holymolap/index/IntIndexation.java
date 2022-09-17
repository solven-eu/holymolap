package eu.solven.holymolap.index;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.ILazyHolyCube;
import eu.solven.holymolap.cube.aggregates.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.cube.aggregates.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.record.FastEntry;

public class IntIndexation {

	@Test
	public void testIndexAllKeysOneByOne() {
		IHolyMeasuresTableDefinition definitions = new EmptyHolyMeasureTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		ImmutableSinkContext context =
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY),
						Collections.emptySet(),
						Collections.emptySet());
		IHolyCube cube = sink.sink(context,
				new FastEntry(ImmutableList.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY),
						new Object[0],
						null,
						new int[] { 3, 7 }));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(2, cube.getCellSet().getAxesWithCoordinates().axes().size());

		if (cube instanceof ILazyHolyCube) {
			ILazyHolyCube lazyCube = (ILazyHolyCube) cube;

			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));
		}

	}
}
