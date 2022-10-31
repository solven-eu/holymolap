package eu.solven.holymolap.measures.aggregation;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IAggregationLogic<T> {

	IBinaryOperator getOperator();

	/**
	 * 
	 * @param measuresTable
	 *            the base aggregates
	 * @param rowsIterator
	 *            the rowIndexes to aggregates
	 * @param coordinateIndexes
	 * @return
	 */
	T aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice);

}
