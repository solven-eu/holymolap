package eu.solven.holymolap.index;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.ILazyHolyCube;
import eu.solven.holymolap.cube.aggregates.EmptyHolyAggregateTableDefinition;
import eu.solven.holymolap.cube.aggregates.IHolyAggregateTableDefinition;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.record.FastEntry;

public class IntIndexation {

	@Test
	public void testIndexAllKeysOneByOne() {
		IHolyAggregateTableDefinition definitions = new EmptyHolyAggregateTableDefinition();
		IHolyCubeSink sink = new HolyCubeSink(definitions);

		ImmutableSinkContext context =
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY),
						Collections.emptySet(),
						Collections.emptySet());
		IHolyCube cube = sink.sink(context, new FastEntry(new Object[0], null, new int[] { 3, 7 }));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(2, cube.getCellSet().axes().size());

		if (cube instanceof ILazyHolyCube) {
			ILazyHolyCube lazyCube = (ILazyHolyCube) cube;

			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
			lazyCube.getCellSet().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));
		}

	}
}
