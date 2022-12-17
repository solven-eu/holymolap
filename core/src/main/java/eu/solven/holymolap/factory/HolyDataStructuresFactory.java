package eu.solven.holymolap.factory;

import eu.solven.holymolap.compression.doubles.DynamicSchemeDoubleList;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.mutable.cellset.IAppendableHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.IBijectiveHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.SwitchOnSizeCellToRow;
import eu.solven.holymolap.mutable.cellset.VariableByteHolyCellToRow;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableLongAggregatesColumn;
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
public class HolyDataStructuresFactory implements IHolyDataStructuresFactory {

	@Override
	public IBijectiveHolyCellToRow makeBijectiveCellToRow() {
		// VariableByteHolyCellToRow costs slightly more in memory than FibonacciEncoding, but it is much faster
		return new VariableByteHolyCellToRow();
	}

	@Override
	public IAppendableHolyCellToRow makeCellToRow() {
		// VariableByteHolyCellToRow costs slightly more in memory than FibonacciEncoding, but it is much faster
		return new SwitchOnSizeCellToRow(() -> makeBijectiveCellToRow());
	}

	@Override
	public OperatorFactory makeOperatorFactory() {
		return new OperatorFactory();
	}

	@Override
	public IMutableAggregatesColumn makeMutableAggregatesColumn(IBinaryOperator binaryOperator) {
		return new MutableAggregatesColumn(binaryOperator);
	}

	@Override
	public IMutableAggregatesColumn makeMutableDoubleAggregatesColumn(IDoubleBinaryOperator binaryOperator) {
		return new MutableDoubleAggregatesColumn(binaryOperator);
	}

	@Override
	public IMutableAggregatesColumn makeMutableLongAggregatesColumn(ILongBinaryOperator binaryOperator) {
		return new MutableLongAggregatesColumn(binaryOperator);
	}

	@Override
	public DoubleList makeDoubleList(DoubleList cellToAggregate) {
		return new DynamicSchemeDoubleList(cellToAggregate);
	}

}
