package eu.solven.holymolap.measures.aggregation;

import eu.solven.holymolap.measures.IHolyMeasuresTable;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IDoubleAggregationLogic extends IAggregationLogic<Double> {
	@Override
	default Double aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes) {
		return aggregateToDouble(measuresTable, rowsIterator, coordinateIndexes);
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
	double aggregateToDouble(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, long[] coordinateIndexes);

}
