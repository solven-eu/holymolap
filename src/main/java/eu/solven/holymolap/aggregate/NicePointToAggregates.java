package eu.solven.holymolap.aggregate;

import java.util.NavigableMap;

public class NicePointToAggregates<T> {
	final NavigableMap<String, Object> keyToValue;
	final T keyToAggregates;

	public NicePointToAggregates(T keyToAggregates, NavigableMap<String, Object> keyToValue) {
		this.keyToAggregates = keyToAggregates;
		this.keyToValue = keyToValue;
	}

	public NavigableMap<String, Object> getKeyToValue() {
		return keyToValue;
	}

	public T getKeyToAggregates() {
		return keyToAggregates;
	}

}
