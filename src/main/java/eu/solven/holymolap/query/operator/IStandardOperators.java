package eu.solven.holymolap.query.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public interface IStandardOperators {
	IDoubleBinaryOperator SUM = new SumDoubleBinaryOperator();
}
