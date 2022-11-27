package eu.solven.holymolap.mutable.axis;

import java.util.Collections;
import java.util.Set;

public class EmptyAxisDictionary implements IMutableAxisSmallDictionary {

	@Override
	public int cardinality() {
		return 0;
	}

	@Override
	public int getIndexMayMiss(Object coordinate) {
		return NO_COORDINATE_INDEX;
	}

	@Override
	public Set<?> orderedCoordinates() {
		return Collections.emptySet();
	}

	@Override
	public boolean isLocked() {
		return true;
	}

	@Override
	public int getIndexMayAppend(Object coordinate) {
		throw new IllegalArgumentException(this.getClass().getName() + " can not be appended");
	}
}
