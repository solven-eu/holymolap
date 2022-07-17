package eu.solven.holymolap.index;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.solven.holymolap.TestAggregation;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.sink.FastEntry;
import eu.solven.holymolap.sink.IHolySink;
import eu.solven.holymolap.sink.ImmutableSinkContext;
import eu.solven.holymolap.sink.RoaringSink;

public class IntIndexation {

	@Test
	public void testIndexAllKeysOneByOne() {
		IHolySink sink = new RoaringSink();

		IHolyCube cube = sink.sink(new FastEntry(new Object[0], null, new int[] { 3, 7 }), new ImmutableSinkContext(
				ImmutableSet.of(TestAggregation.FIRST_KEY, TestAggregation.SECOND_KEY), Collections.emptySet(), Collections.emptySet()));

		// IRoaringCube cube = sink.sink(rows);

		Assert.assertEquals(1, cube.getNbRows());
		Assert.assertEquals(2, cube.getIndex().keySet().size());

		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.FIRST_KEY));
		cube.getIndex().startIndexing(ImmutableSet.of(TestAggregation.SECOND_KEY));

	}
}
