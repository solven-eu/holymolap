package eu.solven.holymolap.measures.aggregation;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface ILongAggregationLogic extends IAggregationLogic<Long> {
	@Override
	ILongBinaryOperator getOperator();

	@Override
	default Long aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice) {
		return aggregateToLong(measuresTable, rowsIterator, slice);
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
	long aggregateToLong(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice);

}