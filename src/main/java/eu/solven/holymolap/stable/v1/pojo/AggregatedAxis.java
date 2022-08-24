package eu.solven.holymolap.stable.v1.pojo;

import java.util.Objects;

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

	@Override
	public int hashCode() {
		return Objects.hash(axis, operator);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof AggregatedAxis)) {
			return false;
		}
		AggregatedAxis other = (AggregatedAxis) obj;
		return Objects.equals(axis, other.axis) && Objects.equals(operator, other.operator);
	}

}
