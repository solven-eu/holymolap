package eu.solven.holymolap.aggregate;

public class RawPointToAggregates<T> {
	public final T keyToAggregates;
	public final long[] coordinatesRef;

	public RawPointToAggregates(T keyToAggregates, long[] coordinatesRef) {
		this.keyToAggregates = keyToAggregates;
		this.coordinatesRef = coordinatesRef;
	}

}
