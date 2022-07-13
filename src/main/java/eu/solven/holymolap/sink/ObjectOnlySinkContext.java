package eu.solven.holymolap.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ObjectOnlySinkContext implements ISinkContext {
	protected final Object[] keys;

	public ObjectOnlySinkContext(Object[] keys) {
		this.keys = keys;
	}

	@Override
	public int expectedNbRows() {
		return 1;
	}

	@Override
	public List<Comparable<?>> keyIndexToKey() {
		return (List) Arrays.asList(keys);
	}

	@Override
	public Set<? extends Comparable<?>> objectKeySet() {
		return new HashSet<>(keyIndexToKey());
	}

	@Override
	public Set<? extends Comparable<?>> doubleKeySet() {
		return Collections.emptySet();
	}

	@Override
	public Set<? extends Comparable<?>> intKeySet() {
		return Collections.emptySet();
	}

	@Override
	public boolean hasNewKey() {
		return false;
	}

}
