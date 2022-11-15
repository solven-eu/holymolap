package eu.solven.holymolap.mutable.axis;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This will materialize a {@link IMutableAxisSmallDictionary} based on the first encountered type. If multiple types
 * are considered, we switch back to an Object-based implementation.
 * 
 * @author Benoit Lacelle
 *
 */
public class LazyTypeAxisDictionary implements ILazyMutableAxisSmallDictionary {
	protected final AtomicReference<IMutableAxisSmallDictionary> underlying = new AtomicReference<>();

	private IMutableAxisSmallDictionary getUnderlying(Class<?> clazz) {
		IMutableAxisSmallDictionary current = underlying.get();

		if (current == null) {
			if (clazz == int.class) {
				current = new Int2IntAxisDictionary();
			} else {
				current = new Object2IntAxisDictionary();
			}
			underlying.set(current);
		}
		return current;
	}

	@Override
	public Set<?> orderedCoordinates() {
		return getUnderlying(Object.class).orderedCoordinates();
	}

	@Override
	public int cardinality() {
		return getUnderlying(Object.class).cardinality();
	}

	@Override
	public int getIndexMayMiss(Object coordinate) {
		return getUnderlying(coordinate.getClass()).getIndexMayMiss(coordinate);
	}

	@Override
	public int getIndexMayAppend(Object coordinate) {
		return getUnderlying(coordinate.getClass()).getIndexMayAppend(coordinate);
	}

	@Override
	public boolean isLocked() {
		return getUnderlying(Object.class).isLocked();
	}

	@Override
	public IMutableAxisSmallDictionary asObjects() {
		return this;
	}

	@Override
	public IMutableAxisSmallIntDictionary asInts() {
		IMutableAxisSmallDictionary current = getUnderlying(int.class);

		if (current instanceof IMutableAxisSmallIntDictionary) {
			return (IMutableAxisSmallIntDictionary) current;
		}

		throw new IllegalStateException("We already turned into an Object-only dictionary");
	}
}
