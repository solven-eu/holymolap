package eu.solven.holymolap.aggregate;

public class RawPointToAggregates<T> {
	public final int[] valueIndexes;
	public final T keyToAggregates;

	public RawPointToAggregates(T keyToAggregates, int[] valueIndexes) {
		this.keyToAggregates = keyToAggregates;
		this.valueIndexes = valueIndexes;
	}

}
