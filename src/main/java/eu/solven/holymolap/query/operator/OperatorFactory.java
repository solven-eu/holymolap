package eu.solven.holymolap.query.operator;

import eu.solven.holymolap.stable.v1.IAggregatedAxis;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.pojo.AggregatedAxis;

public class OperatorFactory {
	public static final String SUM = "SUM";

	public IDoubleBinaryOperator getDoubleBinaryOperator(String key) {
		if (SUM.equalsIgnoreCase(key)) {
			return IStandardOperators.SUM;
		} else {
			throw new IllegalArgumentException("Unknown operator: " + key);
		}
	}

	public static IAggregatedAxis sum(String axis) {
		return new AggregatedAxis(axis, OperatorFactory.SUM);
	}
}
