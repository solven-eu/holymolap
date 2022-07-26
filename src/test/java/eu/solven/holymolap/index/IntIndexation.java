package eu.solven.holymolap.index;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.cube.ILazyHolyCube;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.IHolySink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.HolyCubeSink;

public class IntIndexation {

	@Test
	public void testIndexAllKeysOneByOne() {
		IHolySink sink = new HolyCubeSink();

		ImmutableSinkContext context =
				new ImmutableSinkContext(ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY),
						Collections.emptySet(),
						Collections.emptySet());
		ILazyHolyCube cube = (ILazyHolyCube) sink.sink(context, new FastEntry(new Object[0], null, new int[] { 3, 7 }));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(2, cube.getIndex().keySet().size());

		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));

	}
}
