package eu.solven.holymolap.query;

import org.roaringbitmap.IntIterator;

import eu.solven.holymolap.cube.IHolyCube;

public interface IAggregationLogic<T> {

	T aggregateTo(IHolyCube roaringCube, IntIterator it, long[] valueIndexes);

}
