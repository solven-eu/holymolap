package eu.solven.holymolap.cube.aggregates;

import eu.solven.holymolap.query.IAggregationLogic;
import eu.solven.holymolap.query.SingleColumnAggregationLogic;
import eu.solven.holymolap.query.operator.IOperatorFactory;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAggregatedAxis;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public class HolyMeasureColumnMeta implements IHolyMeasureColumnMeta {
	final IAggregatedAxis aggregatedAxis;

	public HolyMeasureColumnMeta(IAggregatedAxis aggregatedAxis) {
		this.aggregatedAxis = aggregatedAxis;
	}

	@Override
	public String getColumn() {
		return aggregatedAxis.getAxis();
	}

	@Override
	public IAggregationLogic<?> getAggregationLogic() {
		IOperatorFactory operatorFactory = new OperatorFactory();

		IDoubleBinaryOperator operator = operatorFactory.getDoubleBinaryOperator(asAggregatedAxis().getOperator());
		return new SingleColumnAggregationLogic(getColumn(), operator);
	}

	@Override
	public IAggregatedAxis asAggregatedAxis() {
		return aggregatedAxis;
	}

}
