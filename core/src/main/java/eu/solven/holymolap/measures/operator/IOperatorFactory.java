package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

public interface IOperatorFactory {
	IBinaryOperator getBinaryOperator(String operator);

	IDoubleBinaryOperator getDoubleBinaryOperator(String operator);

	ILongBinaryOperator getLongBinaryOperator(String operator);

}
