package eu.solven.holymolap.sink;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ImmutableSinkContext implements ISinkContext {
	protected final Set<?> objectKeys;
	protected final Set<?> doubleKeys;
	protected final Set<?> intKeys;

	protected final List<Object> allKeys;

	// protected final List<?> objectKeysAsList;
	// protected final List<?> doubleKeysAsList;
	// protected final List<Object> intKeysAsList;

	public ImmutableSinkContext(Collection<?> objectKeys, Collection<?> doubleKeys, Collection<Object> intKeys) {
		this.objectKeys = ImmutableSet.copyOf(objectKeys);
		this.doubleKeys = ImmutableSet.copyOf(doubleKeys);
		this.intKeys = ImmutableSet.copyOf(intKeys);

		allKeys = ImmutableList.builder().addAll(doubleKeys).addAll(objectKeys).addAll(intKeys).build();

		if (allKeys.size() != objectKeys.size() + doubleKeys.size() + intKeys.size()) {
			throw new RuntimeException("Some keys appears in several categorisation");
		}

		// this.objectKeysAsList = ImmutableList.copyOf(objectKeys);
		// this.doubleKeysAsList = ImmutableList.copyOf(doubleKeys);
		// this.intKeysAsList = ImmutableList.copyOf(intKeys);
	}

	@Override
	public int expectedNbRows() {
		return 0;
	}

	@Override
	public List<Comparable<?>> keyIndexToKey() {
		return (List) allKeys;
	}

	@Override
	public Set<? extends Comparable<?>> objectKeySet() {
		return (Set) objectKeys;
	}

	@Override
	public Set<? extends Comparable<?>> doubleKeySet() {
		return (Set) doubleKeys;
	}

	@Override
	public Set<? extends Comparable<?>> intKeySet() {
		return (Set) intKeys;
	}

	@Override
	public boolean hasNewKey() {
		return false;
	}

}
