package eu.solven.holymolap.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Deprecated
public class ObjectOnlySinkContext implements ISinkContext {
	protected final String[] keys;

	public ObjectOnlySinkContext(String[] keys) {
		this.keys = keys;
	}

	@Override
	public int expectedNbRows() {
		return 1;
	}

	@Override
	public List<String> keyIndexToKey() {
		return Arrays.asList(keys);
	}

	@Override
	public Set<? extends String> objectKeySet() {
		return new HashSet<>(keyIndexToKey());
	}

	@Override
	public Set<? extends String> doubleKeySet() {
		return Collections.emptySet();
	}

	@Override
	public Set<? extends String> intKeySet() {
		return Collections.emptySet();
	}

	@Override
	public boolean hasNewKey() {
		return false;
	}

}
