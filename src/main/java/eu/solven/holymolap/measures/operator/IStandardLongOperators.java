package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

public interface IStandardLongOperators {

	ILongBinaryOperator COUNT = new CountBinaryOperator();

	ILongBinaryOperator CELLCOUNT = new CountBinaryOperator();

}
