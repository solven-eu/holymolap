package eu.solven.holymolap.stable.v1.pojo;

import eu.solven.holymolap.stable.v1.IAggregatedAxis;

public class AggregatedAxis implements IAggregatedAxis {

	final String axis;
	final String operator;

	public AggregatedAxis(String axis, String operator) {
		this.axis = axis;
		this.operator = operator;
	}

	@Override
	public String getAxis() {
		return axis;
	}

	@Override
	public String getOperator() {
		return operator;
	}

}
