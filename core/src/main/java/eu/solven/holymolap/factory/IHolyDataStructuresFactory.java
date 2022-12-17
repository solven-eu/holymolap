package eu.solven.holymolap.factory;

import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.mutable.cellset.IAppendableHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.IBijectiveHolyCellToRow;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * Provides default implementation for various internal data-structures.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyDataStructuresFactory {
	IAppendableHolyCellToRow makeCellToRow();

	IBijectiveHolyCellToRow makeBijectiveCellToRow();

	OperatorFactory makeOperatorFactory();

	IMutableAggregatesColumn makeMutableAggregatesColumn(IBinaryOperator binaryOperator);

	IMutableAggregatesColumn makeMutableDoubleAggregatesColumn(IDoubleBinaryOperator binaryOperator);

	IMutableAggregatesColumn makeMutableLongAggregatesColumn(ILongBinaryOperator binaryOperator);

	DoubleList makeDoubleList(DoubleList cellToAggregate);

}
