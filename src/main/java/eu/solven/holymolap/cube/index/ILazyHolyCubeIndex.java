package eu.solven.holymolap.cube.index;

import java.util.Set;

public interface ILazyHolyCubeIndex extends IHolyCubeIndex {

	void startIndexing(int axisIndex);

	void startIndexing(Set<String> keysToIndex);

}
