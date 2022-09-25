package eu.solven.holymolap.stable.v1.pojo;

import java.util.Objects;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class MeasuredAxis implements IMeasuredAxis {

	final String axis;
	final String operator;

	public MeasuredAxis(String axis, String operator) {
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
		if (!(obj instanceof MeasuredAxis)) {
			return false;
		}
		MeasuredAxis other = (MeasuredAxis) obj;
		return Objects.equals(axis, other.axis) && Objects.equals(operator, other.operator);
	}

	@Override
	public String toString() {
		return "MeasuredAxis [axis=" + axis + ", operator=" + operator + "]";
	}

}
