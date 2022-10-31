package eu.solven.holymolap.immutable.dictionary;

import java.util.Collection;
import java.util.Collections;

public class EmptyAxisCoordinatesDictionary implements IAxisCoordinatesDictionary {

	@Override
	public long getCoordinateRef(Object value) {
		return NOT_INDEXED;
	}

	@Override
	public Object getCoordinate(long coordinateRef) {
		return NO_REFERENCE;
	}

	@Override
	public Collection<?> coordinates() {
		return Collections.emptySet();
	}

}
