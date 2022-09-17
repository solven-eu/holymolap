package eu.solven.holymolap.stable.v1;

import eu.solven.holymolap.query.operator.IStandardOperators;

/**
 * Express how given axis should be aggregated in an {@link IAggregationQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMeasuredAxis {
	String getAxis();

	/**
	 * See {@link IStandardOperators} for standard operators (like SUM).
	 * 
	 * @return
	 */
	String getOperator();
}
