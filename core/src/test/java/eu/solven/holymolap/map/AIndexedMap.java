package eu.solven.holymolap.map;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Faster than {@link MappedTuple} as it does not need to do a
 * {@link HashMap#get(Object)} for {@link IIndexedMap#getKeyPosition(Object)}
 * 
 * @author Benoit Lacelle
 * 
 */
public abstract class AIndexedMap<S, T> implements IIndexedMap<S, T> {
	private static final Function<Object, String> SAFE_TO_STRING = new Function<Object, String>() {

		@Override
		public String apply(Object input) {
			return String.valueOf(input);
		}
	};
	protected final Set<? extends S> keys;

	// protected final int[] keyIndexes;

	public AIndexedMap(Set<? extends S> keys) {
		this.keys = keys;

		// keyIndexes = new int[keys.size()];
	}

	@Override
	public int size() {
		return keys.size();
	}

	@Override
	public boolean isEmpty() {
		return keys.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return keys.contains(key);
	}

	@Override
	public boolean containsValue(Object value) {
		// http://stackoverflow.com/questions/1128723/in-java-how-can-i-test-if-an-array-contains-a-certain-value
		return Arrays.asList(values()).contains(value);
	}

	@Override
	public T get(Object key) {
		return indexedGet(getKeyPosition(key));
	}

	@Override
	public T put(S key, T value) {
		return putIndexed(getKeyPosition(key), value);
	}

	@Override
	public T remove(Object key) {
		throw new UnsupportedOperationException("Can not remove. But can set null");
	}

	@Override
	public void putAll(Map<? extends S, ? extends T> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("Can not remove. But can set null");
	}

	@Override
	public Set<S> keySet() {
		return Collections.unmodifiableSet(keys);
	}

	@Override
	public Set<Entry<S, T>> entrySet() {
		return new AbstractSet<Entry<S, T>>() {

			@Override
			public Iterator<Map.Entry<S, T>> iterator() {
				final Iterator<? extends S> keyIterator = keys.iterator();

				return new Iterator<Map.Entry<S, T>>() {
					int index = 0;

					@Override
					public boolean hasNext() {
						return keyIterator.hasNext();
					}

					@Override
					public Entry<S, T> next() {
						Entry<S, T> next = new AbstractMap.SimpleImmutableEntry<S, T>(keyIterator.next(), AIndexedMap.this.get(index));

						index++;

						return next;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException("Can not remove. But can set null");
					}
				};
			}

			@Override
			public int size() {
				return AIndexedMap.this.size();
			}
		};
	}

	// @Override
	// public abstract Comparable<?> getKey(int index);

	// @Override
	// public abstract int getKeyPosition(Object key);

	@Override
	public T putIfNull(S key, T value) {
		int index = getKeyPosition(key);

		T currentValue = get(index);

		if (currentValue == null) {
			putIndexed(index, value);
		}

		return currentValue;
	}

	@Override
	public String toString() {
		// TODO: it fails on null value
		Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator("=");
		// Handle null values
		return "{" + mapJoiner.join(Maps.transformValues(this, SAFE_TO_STRING)) + "}";
	}

	// TODO: hashCode equals
}