package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;

public interface IOperatorFactory {
	String SUM = "SUM";
	/**
	 * Count the number of considered input records (similarly to SQL)
	 */
	String COUNT = "COUNT";
	/**
	 * Count the number of cells considered in the query. It helps understanding the granularity of the considered data,
	 * or the presence/lack of intermediate cubes.
	 */
	String CELLCOUNT = "CELLCOUNT";

	IBinaryOperator getBinaryOperator(String operator);

	IDoubleBinaryOperator getDoubleBinaryOperator(String operator);

	ILongBinaryOperator getLongBinaryOperator(String operator);

}
