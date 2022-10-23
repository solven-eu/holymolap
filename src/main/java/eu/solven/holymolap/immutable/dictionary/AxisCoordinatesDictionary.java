package eu.solven.holymolap.immutable.dictionary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

/**
 * An immutable implementation of {@link IAxisCoordinatesDictionary}, based on {@link Object2LongMap} and
 * {@link ArrayList}
 * 
 * @author Benoit Lacelle
 *
 */
public class AxisCoordinatesDictionary implements IAxisCoordinatesDictionary {

	protected final Object2LongMap<Object> valueToIndex;
	// This is need for randomAccess to coordinates from ref
	protected final List<Object> valueIndexToValue;

	public AxisCoordinatesDictionary(Set<?> coordinates) {
		valueToIndex = new Object2LongOpenHashMap<>(coordinates.size());

		// Value indexes will only be positive values: we use a negative default
		// value for detection of values not-indexed yet
		valueToIndex.defaultReturnValue(NOT_INDEXED);

		valueIndexToValue = new ArrayList<>(coordinates.size());

		coordinates.forEach(coordinate -> {
			valueToIndex.put(coordinate, valueToIndex.size());
			valueIndexToValue.add(coordinate);
		});
	}

	@Override
	public long getCoordinateRef(Object value) {
		return valueToIndex.getLong(value);
	}

	@Override
	public Object getCoordinate(long valueIndex) {
		if (valueIndex < 0) {
			return NO_REFERENCE;
		} else if (valueIndex >= valueIndexToValue.size()) {
			throw new IllegalArgumentException(
					"There is no coordinate for ref=" + valueIndex + " as max refIndex is " + valueIndexToValue.size());
		}
		return valueIndexToValue.get(Ints.checkedCast(valueIndex));
	}

	@Override
	public Collection<?> coordinates() {
		return Collections.unmodifiableCollection(valueToIndex.values());
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("cardinality", valueIndexToValue.size())
				.add("firstCoodinates", valueIndexToValue.subList(0, Math.min(100, valueIndexToValue.size())))
				.toString();
	}
}
