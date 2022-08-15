package eu.solven.holymolap.sink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class AxisCoordinatesDictionary implements IAxisCoordinatesDictionary {

	protected final Object2LongMap<Object> valueToIndex;
	protected final List<Object> valueIndexToValue;

	public AxisCoordinatesDictionary() {
		valueToIndex = new Object2LongOpenHashMap<>();

		// Value indexes will only be positive values: we use a negative default
		// value for detection of values not-indexed yet
		valueToIndex.defaultReturnValue(NOT_INDEXED);

		valueIndexToValue = new ArrayList<>();
	}

	@Override
	public long mapCoordinateIndex(Object value) {
		long valueIndex = getCoordinateIndex(value);

		if (valueIndex == NOT_INDEXED) {
			valueIndex = valueToIndex.size();
			valueToIndex.put(value, valueIndex);
			valueIndexToValue.add(value);
		}

		return valueIndex;
	}

	@Override
	public long getCoordinateIndex(Object value) {
		return valueToIndex.getLong(value);
	}

	@Override
	public Object getCoordinate(long valueIndex) {
		if (valueIndex < 0) {
			return NO_REFERENCE;
		}
		return valueIndexToValue.get(Ints.checkedCast(valueIndex));
	}

	@Override
	public List<?> coordinates() {
		return Collections.unmodifiableList(valueIndexToValue);
	}
}
