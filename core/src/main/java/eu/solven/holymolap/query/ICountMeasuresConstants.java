package eu.solven.holymolap.query;

import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

public interface ICountMeasuresConstants {

	// Typically used in a COUNT(*) query
	String STAR = "*";

	IMeasuredAxis COUNT_MEASURE = new MeasuredAxis(STAR, IStandardOperators.COUNT);
}
