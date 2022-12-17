package eu.solven.holymolap.mutable.column;

import eu.solven.holymolap.factory.IHolyDataStructuresFactory;
import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;
import eu.solven.holymolap.primitives.HolyPrimitiveParser;

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
		double asDouble = HolyPrimitiveParser.toDouble(object);
		aggregateDouble(cellIndex, asDouble);
	}

	void aggregateDouble(int rowIndex, double contribution);

	@Override
	IScannableDoubleMeasureColumn flush(IHolyDataStructuresFactory factory);

}
