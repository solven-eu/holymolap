package eu.solven.holymolap.cube;

import java.util.NavigableSet;

import eu.solven.holymolap.stable.v1.IHasAxes;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

public interface IHasNavigableAxes extends IHasAxes, IHasMemoryFootprint {

	NavigableSet<String> axes();

	String indexToAxis(int axisIndex);

	int getAxisIndex(String axis);
}
