package eu.solven.holymolap.axes;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;

public class EmptyAxisWithCoordinates implements IHasAxesWithCoordinates {

	@Override
	public NavigableSet<String> axes() {
		return Collections.emptyNavigableSet();
	}

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public String indexToAxis(int axisIndex) {
		throw new IllegalArgumentException("Empty");
	}

	@Override
	public int getAxisIndex(String axis) {
		return -1;
	}

	@Override
	public long getSizeInBytes() {
		return 0;
	}

	@Override
	public Object dereferenceCoordinate(int axisIndex, long coordinateRef) {
		throw new IllegalArgumentException("Empty");
	}

	@Override
	public long getCoordinateRef(int axisIndex, Object coordinate) {
		return -1;
	}

}
