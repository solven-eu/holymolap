package eu.solven.holymolap.query;

import eu.solven.holymolap.cube.aggregates.IHolyMeasureTable;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IAggregationLogic<T> {

	/**
	 * 
	 * @param measuresTable
	 *            the base aggregates
	 * @param rowsIterator
	 *            the rowIndexes to aggregates
	 * @param coordinateIndexes
	 * @return
	 */
	T aggregateTo(IHolyMeasureTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes);

}
