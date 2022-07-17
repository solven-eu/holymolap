package eu.solven.holymolap.query.operator;

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;

public class DoubleAggregate implements IDoubleAggregate {

	protected final IDoubleBinaryOperator operator;

	protected final AtomicBoolean hasBeenInitialized = new AtomicBoolean(false);
	protected final AtomicDouble currentAggregate = new AtomicDouble();

	public DoubleAggregate(IDoubleBinaryOperator operator) {
		this.operator = operator;
	}

	public DoubleAggregate(IDoubleBinaryOperator operator, double doubleValue) {
		this(operator);

		hasBeenInitialized.set(true);
		currentAggregate.set(doubleValue);
	}

	@Override
	public IDoubleBinaryOperator operator() {
		return operator;
	}

	@Override
	public boolean isNullAggregate() {
		return hasBeenInitialized.get();
	}

	@Override
	public double getCurrentAggregate() {
		return currentAggregate.get();
	}

	@Override
	public void contribute(double newValue) {
		if (hasBeenInitialized.compareAndSet(false, true)) {
			currentAggregate.set(newValue);
		} else {
			double current = currentAggregate.get();
			boolean result = currentAggregate.compareAndSet(current, operator.applyAsDouble(current, newValue));

			if (!result) {
				while (!result) {
					current = currentAggregate.get();
					result = currentAggregate.compareAndSet(current, operator.applyAsDouble(current, newValue));
				}
			}
		}
	}

	@Override
	public String toString() {
		if (hasBeenInitialized.get()) {
			return Double.toString(currentAggregate.get());
		} else {
			return "Empty";
		}
	}

	private static int doubleHashCode(double value) {
		long bits = Double.doubleToLongBits(value);
		return (int) (bits ^ (bits >>> 32));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((currentAggregate == null) ? 0 : doubleHashCode(currentAggregate.hashCode()));
		result = prime * result + ((hasBeenInitialized == null) ? 0 : hasBeenInitialized.hashCode());
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DoubleAggregate other = (DoubleAggregate) obj;
		if (currentAggregate == null) {
			if (other.currentAggregate != null)
				return false;
		} else if (Double.doubleToLongBits(currentAggregate.doubleValue()) != Double.doubleToLongBits(other.currentAggregate.get()))
			return false;
		if (hasBeenInitialized == null) {
			if (other.hasBeenInitialized != null)
				return false;
		} else if (hasBeenInitialized.get() != other.hasBeenInitialized.get())
			return false;
		if (operator == null) {
			if (other.operator != null)
				return false;
		} else if (!operator.equals(other.operator))
			return false;
		return true;
	}

}
