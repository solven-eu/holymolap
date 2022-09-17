package eu.solven.holymolap.cube.measures;

import eu.solven.holymolap.query.IAggregationLogic;
import eu.solven.holymolap.query.SingleColumnAggregationLogic;
import eu.solven.holymolap.query.operator.IOperatorFactory;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class HolyMeasureColumnMeta implements IHolyMeasureColumnMeta {
	final IMeasuredAxis aggregatedAxis;

	final IAggregationLogic<Double> aggregationLogic;

	/**
	 * 
	 * @param measuredAxis
	 * @param measureIndex
	 *            the measureIndex of given {@link IMeasuredAxis} in some IHolyMeasuresTableDefinition
	 */
	public HolyMeasureColumnMeta(IMeasuredAxis measuredAxis, int measureIndex) {
		this.aggregatedAxis = measuredAxis;

		IOperatorFactory operatorFactory = new OperatorFactory();

		IDoubleBinaryOperator operator = operatorFactory.getDoubleBinaryOperator(asMeasuredAxis().getOperator());
		this.aggregationLogic = new SingleColumnAggregationLogic(measureIndex, operator);
	}

	@Override
	public IAggregationLogic<?> getAggregationLogic() {
		return aggregationLogic;
	}

	@Override
	public IMeasuredAxis asMeasuredAxis() {
		return aggregatedAxis;
	}

}
