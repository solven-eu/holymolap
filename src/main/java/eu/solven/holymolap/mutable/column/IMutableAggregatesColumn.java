package eu.solven.holymolap.mutable.column;

import eu.solven.holymolap.immutable.column.IScannableMeasureColumn;

/**
 * Enable accumulating things
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableAggregatesColumn {

	void aggregateObject(int cellIndex, Object doubleValue);

	IScannableMeasureColumn flush();

}
