package eu.solven.holymolap.query.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public interface IOperatorFactory {

	IDoubleBinaryOperator getDoubleBinaryOperator(String operator);

}
