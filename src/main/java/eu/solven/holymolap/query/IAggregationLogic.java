package eu.solven.holymolap.query;

import eu.solven.holymolap.cube.IHolyCube;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IAggregationLogic<T> {

	T aggregateTo(IHolyCube roaringCube, LongIterator rowsIterator, long[] valueIndexes);

}
