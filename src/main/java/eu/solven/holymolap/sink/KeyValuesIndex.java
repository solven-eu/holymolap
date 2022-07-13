package eu.solven.holymolap.sink;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class KeyValuesIndex implements IKeyValuesIndex {

	protected final Object2IntMap<Object> valueToIndex;
	protected final List<Object> valueIndexToValue;

	public KeyValuesIndex() {
		valueToIndex = new Object2IntOpenHashMap<>();

		// Value indexes will only be positive values: we use a negative default
		// value for detection of values not-indexed yet
		valueToIndex.defaultReturnValue(NOT_INDEXED);

		valueIndexToValue = new ArrayList<>();
	}

	@Override
	public int mapValueIndex(Object value) {
		int valueIndex = valueToIndex.getInt(value);

		if (valueIndex == NOT_INDEXED) {
			valueIndex = valueToIndex.size();
			valueToIndex.put(value, valueIndex);
			valueIndexToValue.add(value);
		}

		return valueIndex;
	}

	@Override
	public int getValueIndex(Object value) {
		return valueToIndex.getInt(value);
	}

	@Override
	public Object getValue(int valueIndex) {
		return valueIndexToValue.get(valueIndex);
	}

	@Override
	public List<?> values() {
		return Collections.unmodifiableList(valueIndexToValue);
	}
}
