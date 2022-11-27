package eu.solven.holymolap.factory;

import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.mutable.cellset.IHolyCellToRow;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

/**
 * Provides default implementation for various internal data-structures.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyDataStructuresFactory {
	IHolyCellToRow makeCellToRow();

	OperatorFactory makeOperatorFactory();

	IMutableAggregatesColumn makeMutableAggregatesColumn(IBinaryOperator binaryOperator);

	IMutableAggregatesColumn makeMutableDoubleAggregatesColumn(IDoubleBinaryOperator binaryOperator);

	IMutableAggregatesColumn makeMutableLongAggregatesColumn(ILongBinaryOperator binaryOperator);

}
