package eu.solven.holymolap.measures.operator;

import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public interface IStandardDoubleOperators {

	IDoubleBinaryOperator SUM = new SumDoubleBinaryOperator();

	@Deprecated(since = "avg should be computed as the ratio of SUM / COUNT")
	IBinaryOperator AVG = new AvgDoubleBinaryOperator();

}
