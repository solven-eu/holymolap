package eu.solven.holymolap.aggregate;

import java.util.NavigableMap;

public class NicePointToAggregates<T> {
	public final NavigableMap<Object, Object> keyToValue;
	public final T keyToAggregates;

	public NicePointToAggregates(T keyToAggregates, NavigableMap<Object, Object> keyToValue) {
		this.keyToAggregates = keyToAggregates;
		this.keyToValue = keyToValue;
	}

}
