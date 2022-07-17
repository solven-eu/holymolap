package eu.solven.holymolap.stable.v1;

/**
 * Express how given axis should be aggregated in an {@link IAggregationQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregatedAxis {
	String getAxis();

	/**
	 * See {@link IDoubleBinaryOperator} for standard operators.
	 * 
	 * @return
	 */
	String getOperator();
}
