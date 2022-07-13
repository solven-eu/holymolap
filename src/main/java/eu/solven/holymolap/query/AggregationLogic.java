package eu.solven.holymolap.query;

import org.roaringbitmap.IntIterator;

import eu.solven.holymolap.IHolyCube;

public interface AggregationLogic<T> {

	T aggregateTo(IHolyCube roaringCube, IntIterator it, int[] valueIndexes);

}
