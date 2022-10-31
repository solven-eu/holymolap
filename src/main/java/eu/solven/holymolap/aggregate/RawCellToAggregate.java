package eu.solven.holymolap.aggregate;

public class RawCellToAggregate<T> {
	final CoordinatesRefs coordinates;
	final T measureValue;

	public RawCellToAggregate(CoordinatesRefs coordinates, T measureValue) {
		this.coordinates = coordinates;
		this.measureValue = measureValue;
	}

	public CoordinatesRefs getCoordinatesRefs() {
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
