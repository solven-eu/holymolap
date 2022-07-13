package eu.solven.holymolap.query.operator;

public interface IDoubleAggregate {
	IDoubleBinaryOperator operator();

	boolean isNullAggregate();

	double getCurrentAggregate();

	void contribute(double value);
}
