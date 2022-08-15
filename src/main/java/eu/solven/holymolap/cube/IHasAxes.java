package eu.solven.holymolap.cube;

import java.util.NavigableSet;

public interface IHasAxes {

	NavigableSet<String> axes();

	String indexToAxis(int axisIndex);

	int getAxisIndex(String axis);
}
