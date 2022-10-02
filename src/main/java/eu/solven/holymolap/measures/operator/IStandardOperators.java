package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

public interface IStandardOperators {
	IDoubleBinaryOperator SUM = new SumDoubleBinaryOperator();

	ILongBinaryOperator COUNT = new CountBinaryOperator();

	ILongBinaryOperator CELLCOUNT = new CountBinaryOperator();

}
