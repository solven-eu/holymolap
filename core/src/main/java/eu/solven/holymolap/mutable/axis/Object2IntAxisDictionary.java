package eu.solven.holymolap.mutable.axis;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

public class Object2IntAxisDictionary implements IMutableAxisSmallDictionary {
	private static final Logger LOGGER = LoggerFactory.getLogger(Object2IntAxisDictionary.class);

	private static Set<Class<?>> VALID_CLASSES = Sets.newConcurrentHashSet();

	protected final Object2IntMap<Object> coordinateToIndex;
	protected final AtomicBoolean locked = new AtomicBoolean();

	private static boolean isValidCoordinate(Object coordinate) {
		if (coordinate == null) {
			// TODO Is it legit to have null coordinates ?
			return true;
		}

		Class<? extends Object> coordinateClass = coordinate.getClass();
		if (VALID_CLASSES.contains(coordinateClass)) {
			return true;
		}

		// First-encounter or invalid class
		try {
			Method hashcodeMethod = coordinateClass.getMethod("hashCode");
			if (hashcodeMethod.getDeclaringClass() == Object.class) {
				LOGGER.debug(".hasCode is from Object.class for {}", coordinateClass);
				return false;
			}
		} catch (NoSuchMethodException | SecurityException e) {
			LOGGER.error("Issue fetching .hashCode method from {}", coordinateClass);
			return false;
		}

		VALID_CLASSES.add(coordinateClass);

		return true;
	}

	/**
	 * 
	 * @param coordinateToIndex
	 *            This has to be a Linked data-structure, as it will be returned by .orderedCoordinates()
	 */
	protected Object2IntAxisDictionary(Object2IntMap<Object> coordinateToIndex) {
		this.coordinateToIndex = coordinateToIndex;

		int defaultReturnValue = coordinateToIndex.defaultReturnValue();
		if (NO_COORDINATE_INDEX != defaultReturnValue) {
			throw new IllegalArgumentException("Illegal defaultReturnValue: " + defaultReturnValue);
		}
	}

	public Object2IntAxisDictionary() {
		this.coordinateToIndex = new Object2IntLinkedOpenHashMap<>();
		this.coordinateToIndex.defaultReturnValue(NO_COORDINATE_INDEX);
	}

	@Override
	public Set<?> orderedCoordinates() {
		locked.set(true);

		return Collections.unmodifiableSet(coordinateToIndex.keySet());
	}

	@Override
	public synchronized int cardinality() {
		return coordinateToIndex.size();
	}

	@Override
	public synchronized int getIndexMayMiss(Object coordinate) {
		return coordinateToIndex.getInt(coordinate);
	}

	@Override
	public synchronized int getIndexMayAppend(Object coordinate) {
		if (locked.get()) {
			throw new IllegalStateException("Should not mutate once locked");
		}

		assert isValidCoordinate(coordinate);

		int coordinateIndex = getIndexMayMiss(coordinate);

		if (coordinateIndex == IAxisSmallDictionary.NO_COORDINATE_INDEX) {
			int previousCardinality = coordinateToIndex.size();
			// This is the first row with given coordinate
			coordinateToIndex.put(coordinate, previousCardinality);

			coordinateIndex = previousCardinality;
		}

		return coordinateIndex;
	}

	@Override
	public boolean isLocked() {
		return locked.get();
	}
}
