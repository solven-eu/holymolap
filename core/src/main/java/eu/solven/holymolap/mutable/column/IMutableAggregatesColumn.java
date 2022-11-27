package eu.solven.holymolap.mutable.column;

import eu.solven.holymolap.immutable.column.IScannableMeasureColumn;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * Enable accumulating things
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableAggregatesColumn extends IHasMemoryFootprint {

	void aggregateObject(int cellIndex, Object doubleValue);

	IScannableMeasureColumn flush();

}
