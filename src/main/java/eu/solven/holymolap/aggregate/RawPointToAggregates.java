package eu.solven.holymolap.aggregate;

public class RawPointToAggregates<T> {
	final long[] coordinatesRef;
	final T measureValue;

	public RawPointToAggregates(long[] coordinatesRef, T measureValue) {
		this.coordinatesRef = coordinatesRef;
		this.measureValue = measureValue;
	}

	public T getMeasureValue() {
		return measureValue;
	}

	public long[] getCoordinatesRef() {
		return coordinatesRef;
	}

}
