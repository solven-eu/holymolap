package eu.solven.holymolap.query;

import eu.solven.holymolap.cube.measures.IHolyMeasuresTable;
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
	T aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes);

}
