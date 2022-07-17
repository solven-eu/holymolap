package eu.solven.holymolap.aggregate;

import java.util.NavigableMap;

public class NicePointToAggregates<T> {
	public final NavigableMap<String, Object> keyToValue;
	public final T keyToAggregates;

	public NicePointToAggregates(T keyToAggregates, NavigableMap<String, Object> keyToValue) {
		this.keyToAggregates = keyToAggregates;
		this.keyToValue = keyToValue;
	}

}
