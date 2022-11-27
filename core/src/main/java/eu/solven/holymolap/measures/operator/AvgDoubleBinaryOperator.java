package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

/**
 * Average over {@link Double}
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated(since = "avg should be computed as the ratio of SUM / COUNT")
public class AvgDoubleBinaryOperator implements IDoubleBinaryOperator {

	@Override
	public double applyAsDouble(double left, double right) {
		// BEWARE this AVG computation is dumb
		return (left + right) / 2D;
	}

	@Override
	public double neutralAsDouble() {
		return 0D;
	}
}
