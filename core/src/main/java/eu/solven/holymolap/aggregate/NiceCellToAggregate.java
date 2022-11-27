package eu.solven.holymolap.aggregate;

import java.util.NavigableMap;

public class NiceCellToAggregate<T> {
	final NavigableMap<String, Object> coordinates;
	final T aggregate;

	public NiceCellToAggregate(T aggregate, NavigableMap<String, Object> coordinates) {
		this.aggregate = aggregate;
		this.coordinates = coordinates;
	}

	public NavigableMap<String, Object> getCoordinates() {
		return coordinates;
	}

	public T getAggregate() {
		return aggregate;
	}

}
