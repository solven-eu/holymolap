package eu.solven.holymolap.measures.aggregation;

import eu.solven.holymolap.measures.IHolyMeasuresTable;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface ILongAggregationLogic extends IAggregationLogic<Long> {
	@Override
	default Long aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes) {
		return aggregateToLong(measuresTable, rowsIterator, coordinateIndexes);
	}

	/**
	 * 
	 * @param measuresTable
	 *            the base aggregates
	 * @param rowsIterator
	 *            the rowIndexes to aggregates
	 * @param coordinateIndexes
	 * @return
	 */
	long aggregateToLong(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes);

}