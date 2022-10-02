package eu.solven.holymolap.mutable.column;

import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;

/**
 * Enable accumulating doubles
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableDoubleAggregatesColumn extends IMutableAggregatesColumn {

	@Override
	@Deprecated
	default void aggregateObject(int cellIndex, Object object) {
		aggregateDouble(cellIndex, (((Number) object).doubleValue()));
	}

	void aggregateDouble(int rowIndex, double contribution);

	@Override
	IScannableDoubleMeasureColumn flush();

}
