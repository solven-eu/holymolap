package eu.solven.holymolap.aggregate;

public class RawPointToAggregates<T> {
	public final long[] valueIndexes;
	public final T keyToAggregates;

	public RawPointToAggregates(T keyToAggregates, long[] valueIndexes) {
		this.keyToAggregates = keyToAggregates;
		this.valueIndexes = valueIndexes;
	}

}
