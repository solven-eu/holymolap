package eu.solven.holymolap.aggregate;

public class RawCellToAggregate<T> {
	final long[] coordinatesRef;
	final T measureValue;

	public RawCellToAggregate(long[] coordinatesRef, T measureValue) {
		this.coordinatesRef = coordinatesRef;
		this.measureValue = measureValue;
	}

	public long[] getCoordinatesRef() {
		return coordinatesRef;
	}

	public T getMeasureValue() {
		return measureValue;
	}

}
