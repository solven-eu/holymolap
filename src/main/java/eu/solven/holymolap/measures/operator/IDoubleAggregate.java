package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public interface IDoubleAggregate {
	IDoubleBinaryOperator operator();

	boolean isNullAggregate();

	double getCurrentAggregate();

	void contribute(double value);
}
