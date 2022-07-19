package eu.solven.holymolap.sink;

import java.util.List;

public interface IKeyValuesIndex {
	int NOT_INDEXED = -1;

	/**
	 * 
	 * @param value
	 * @return the index for given coordinate, else NOT_INDEXED
	 */
	long getValueIndex(Object value);

	/**
	 * 
	 * @param value
	 * @return the index of given coordinate, indexing it if it is not yet indexed
	 */
	long mapValueIndex(Object value);

	Object getValue(long valueIndex);

	List<?> values();
}
