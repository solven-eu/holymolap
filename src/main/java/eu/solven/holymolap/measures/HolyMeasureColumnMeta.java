package eu.solven.holymolap.measures;

import eu.solven.holymolap.measures.aggregation.DoubleAggregationLogic;
import eu.solven.holymolap.measures.aggregation.IAggregationLogic;
import eu.solven.holymolap.measures.aggregation.LongAggregationLogic;
import eu.solven.holymolap.measures.aggregation.ObjectAggregationLogic;
import eu.solven.holymolap.measures.operator.IOperatorFactory;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class HolyMeasureColumnMeta implements IHolyMeasureColumnMeta {
	final IMeasuredAxis measuredAxis;

	final IAggregationLogic<?> aggregationLogic;

	/**
	 * 
	 * @param measuredAxis
	 * @param measureIndex
	 *            the measureIndex of given {@link IMeasuredAxis} in some IHolyMeasuresTableDefinition
	 */
	public HolyMeasureColumnMeta(IMeasuredAxis measuredAxis, int measureIndex) {
		this.measuredAxis = measuredAxis;

		IOperatorFactory operatorFactory = new OperatorFactory();

		IBinaryOperator operator = operatorFactory.getBinaryOperator(asMeasuredAxis().getOperator());

		if (operator instanceof IDoubleBinaryOperator) {
			this.aggregationLogic = new DoubleAggregationLogic(measureIndex, (IDoubleBinaryOperator) operator);
		} else if (operator instanceof ILongBinaryOperator) {
			this.aggregationLogic = new LongAggregationLogic(measureIndex, (ILongBinaryOperator) operator);
		} else {
			this.aggregationLogic = new ObjectAggregationLogic(measureIndex, operator);
		}
	}

	@Override
	public IAggregationLogic<?> getAggregationLogic() {
		return aggregationLogic;
	}

	@Override
	public IMeasuredAxis asMeasuredAxis() {
		return measuredAxis;
	}

}
