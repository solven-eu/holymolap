package eu.solven.holymolap.cube;

import java.util.NavigableSet;

import eu.solven.holymolap.stable.v1.IHasAxes;

public interface IHasNavigableAxes extends IHasAxes {

	NavigableSet<String> axes();

	String indexToAxis(int axisIndex);

	int getAxisIndex(String axis);
}
