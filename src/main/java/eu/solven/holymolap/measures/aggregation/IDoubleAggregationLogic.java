package eu.solven.holymolap.measures.aggregation;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IDoubleAggregationLogic extends IAggregationLogic<Double> {
	@Override
	IDoubleBinaryOperator getOperator();

	@Override
	default Double aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice) {
		return aggregateToDouble(measuresTable, rowsIterator, slice);
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
	double aggregateToDouble(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice);

}
