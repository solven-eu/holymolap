package eu.solven.holymolap.cube;

import java.util.NavigableSet;

public interface IHasAxes {

	NavigableSet<String> keySet();

	String indexToAxis(int keyIndex);

	int getKeyIndex(String wildcardKey);
}
