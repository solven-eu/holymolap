package eu.solven.holymolap.sink;

import java.util.List;
import java.util.Set;

/**
 * When sinking some data, we enable recording some context. Typically, some metadata about each column.
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated
public interface ISinkContext {
	int expectedNbRows();

	List<String> keyIndexToKey();

	Set<? extends String> objectKeySet();

	Set<? extends String> doubleKeySet();

	Set<? extends String> intKeySet();

	boolean hasNewKey();

}
