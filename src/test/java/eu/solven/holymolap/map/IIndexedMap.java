package eu.solven.holymolap.map;

import java.util.Map;

public interface IIndexedMap<S, T> extends Map<S, T> {
	double getDouble(Object key);

	double indexedGetDouble(int index);

	T indexedGet(int index);

	// S getKey(int index);

	T putIfNull(S key, T value);

	/**
	 * Like {@link Map}, we accept any Object
	 * 
	 * @param key
	 * @return
	 */
	int getKeyPosition(Object key);

	T putIndexed(int index, T value);
}
