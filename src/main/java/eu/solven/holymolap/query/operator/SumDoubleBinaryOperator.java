package eu.solven.holymolap.query.operator;

public class SumDoubleBinaryOperator implements IDoubleBinaryOperator {

	@Override
	public double applyAsDouble(double left, double right) {
		return left + right;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj instanceof SumDoubleBinaryOperator) {
			if (this.getClass() == obj.getClass()) {
				return true;
			}
		}

		return false;
	}

	@Override
	public double neutral() {
		return 0D;
	}
}
