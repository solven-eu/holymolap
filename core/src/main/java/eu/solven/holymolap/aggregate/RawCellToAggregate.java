package eu.solven.holymolap.aggregate;

public class RawCellToAggregate<T> {
	final ICoordinatesRefs coordinates;
	final T measureValue;

	public RawCellToAggregate(ICoordinatesRefs coordinates, T measureValue) {
		this.coordinates = coordinates;
		this.measureValue = measureValue;
	}

	public ICoordinatesRefs getCoordinatesRefs() {
		return coordinates;
	}

	public T getMeasureValue() {
		return measureValue;
	}

	@Override
	public String toString() {
		return "RawCellToAggregate [coordinates=" + coordinates + ", measureValue=" + measureValue + "]";
	}

}
