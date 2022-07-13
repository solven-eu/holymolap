package eu.solven.holymolap.sink;

import java.util.List;

public interface IKeyValuesIndex {
	int NOT_INDEXED = -1;

	int mapValueIndex(Object value);

	int getValueIndex(Object value);

	Object getValue(int valueIndex);

	List<?> values();
}
