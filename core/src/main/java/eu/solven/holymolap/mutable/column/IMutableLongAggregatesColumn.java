package eu.solven.holymolap.mutable.column;

import eu.solven.holymolap.factory.IHolyDataStructuresFactory;
import eu.solven.holymolap.immutable.column.IScannableLongMeasureColumn;

/**
 * Enable accumulating doubles
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableLongAggregatesColumn extends IMutableAggregatesColumn {

	@Override
	@Deprecated
	default void aggregateObject(int cellIndex, Object object) {
		aggregateLong(cellIndex, (((Number) object).longValue()));
	}

	void aggregateLong(int rowIndex, long contribution);

	@Override
	IScannableLongMeasureColumn flush(IHolyDataStructuresFactory factory);

}
