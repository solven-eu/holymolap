package eu.solven.holymolap.map;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * Faster than {@link MappedTuple} as it does not need to do a
 * {@link HashMap#get(Object)} for {@link IIndexedMap#getKeyPosition(Object)}
 * 
 * @author Benoit Lacelle
 * 
 */
public class ArrayIndexedMap<S, T> extends AIndexedMap<S, T> {

	protected final T[] tuple;
	protected final double[] doubles;

	// protected final TIntObjectMap<Comparable<?>> indexToKey;

	public ArrayIndexedMap(Set<? extends S> keys, T[] tuple) {
		this(keys, tuple, new double[0]);
	}

	public ArrayIndexedMap(Set<? extends S> keys, T[] tuple, double[] doubles) {
		super(keys);

		if (tuple.length + doubles.length == size()) {
			this.tuple = tuple;
			this.doubles = doubles;
		} else {
			throw new IllegalArgumentException("Invalid number of values");
		}
	}

	@Override
	public List<T> values() {
		return Arrays.asList(tuple);
	}

	@Override
	public T indexedGet(int index) {
		if (index < 0) {
			return null;
		} else {
			return tuple[index];
		}
	}

	@Override
	public T putIndexed(int index, T value) {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException(index);
		} else {
			T oldValue = tuple[index];

			tuple[index] = value;

			return oldValue;
		}
	}

	@Override
	public double getDouble(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public double indexedGetDouble(int index) {
		return doubles[index - tuple.length];
	}

	// @Override
	// public Comparable<?> getKey(int index) {
	// // TODO Auto-generated method stub
	// return null;
	// }

	@Override
	public int getKeyPosition(Object key) {
		// TODO: fast index
		return Iterables.indexOf(keys, Predicates.equalTo(key));
	}

}