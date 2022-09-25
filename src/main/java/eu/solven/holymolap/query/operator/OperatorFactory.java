package eu.solven.holymolap.query.operator;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public class OperatorFactory implements IOperatorFactory {
	public static final String SUM = "SUM";

	@Override
	public IDoubleBinaryOperator getDoubleBinaryOperator(String operator) {
		if (SUM.equalsIgnoreCase(operator)) {
			return IStandardOperators.SUM;
		} else {
			throw new IllegalArgumentException("Unknown operator: " + operator);
		}
	}

	public static IMeasuredAxis sum(String axis) {
		return new MeasuredAxis(axis, OperatorFactory.SUM);
	}
}
