package eu.solven.holymolap.aggregate;

import eu.solven.holymolap.immutable.axes.EmptyAxisWithCoordinates;

public class EmptyCoordinatesRefs extends CoordinatesRefs {
	public static EmptyCoordinatesRefs EMPTY = new EmptyCoordinatesRefs();

	public EmptyCoordinatesRefs() {
		super(new EmptyAxisWithCoordinates(), new int[0], new long[0]);
	}

}
