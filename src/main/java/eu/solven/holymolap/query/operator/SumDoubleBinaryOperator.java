package eu.solven.holymolap.query.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

/**
 * Sum over {@link Double}
 * 
 * @author Benoit Lacelle
 *
 */
public class SumDoubleBinaryOperator implements IDoubleBinaryOperator {

	@Override
	public double applyAsDouble(double left, double right) {
		return left + right;
	}

	@Override
	public double neutral() {
		return 0D;
	}
}
