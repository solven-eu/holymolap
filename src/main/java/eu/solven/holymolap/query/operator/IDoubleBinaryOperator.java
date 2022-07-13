package eu.solven.holymolap.query.operator;

/**
 * Represents an operation upon two {@code double}-valued operands, producing a {@code double}-valued result.
 */
public interface IDoubleBinaryOperator {
	IDoubleBinaryOperator SUM = new SumDoubleBinaryOperator();

	double neutral();

	/**
	 * Applies this operator to the given operands.
	 *
	 * @param left
	 *            the first operand
	 * @param right
	 *            the second operand
	 * @return the operator result
	 */
	double applyAsDouble(double left, double right);
}
