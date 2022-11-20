package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public class OperatorFactory implements IOperatorFactory, IStandardOperators {

	@Override
	public IBinaryOperator getBinaryOperator(String operator) {
		if (SUM.equalsIgnoreCase(operator)) {
			return IStandardDoubleOperators.SUM;
		} else if (COUNT.equalsIgnoreCase(operator)) {
			return IStandardLongOperators.COUNT;
		} else if (AVG.equalsIgnoreCase(operator)) {
			return IStandardDoubleOperators.AVG;
		} else {
			throw new IllegalArgumentException("Unknown operator: " + operator);
		}
	}

	@Override
	public IDoubleBinaryOperator getDoubleBinaryOperator(String operator) {
		if (SUM.equalsIgnoreCase(operator)) {
			return IStandardDoubleOperators.SUM;
			// } else if (COUNT.equalsIgnoreCase(operator)) {
			// return IStandardOperators.COUNT;
		} else {
			throw new IllegalArgumentException("Unknown operator: " + operator);
		}
	}

	@Override
	public ILongBinaryOperator getLongBinaryOperator(String operator) {
		if (SUM.equalsIgnoreCase(operator)) {
			return IStandardLongOperators.SUM;
		} else if (COUNT.equalsIgnoreCase(operator)) {
			return IStandardLongOperators.COUNT;
		} else {
			throw new IllegalArgumentException("Unknown operator: " + operator);
		}
	}

	public static IMeasuredAxis sum(String axis) {
		return new MeasuredAxis(axis, OperatorFactory.SUM);
	}

	public static IMeasuredAxis count(String axis) {
		return new MeasuredAxis(axis, OperatorFactory.COUNT);
	}

	public static IMeasuredAxis cellCount(String axis) {
		return new MeasuredAxis(axis, OperatorFactory.CELLCOUNT);
	}

}
