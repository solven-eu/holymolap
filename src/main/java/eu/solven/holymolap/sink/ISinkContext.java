package eu.solven.holymolap.sink;

import java.util.List;
import java.util.Set;

public interface ISinkContext {
	int expectedNbRows();

	List<Comparable<?>> keyIndexToKey();

	Set<? extends Comparable<?>> objectKeySet();

	Set<? extends Comparable<?>> doubleKeySet();

	Set<? extends Comparable<?>> intKeySet();

	boolean hasNewKey();

}
