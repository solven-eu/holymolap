package eu.solven.holymolap.axes;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;

public class AxisWithCoordinates implements IHasAxesWithCoordinates {
	protected final List<? extends String> axisIndexToAxis;
	protected final List<? extends IAxisCoordinatesDictionary> axisIndexToCoordinateIndex;

	public AxisWithCoordinates(List<? extends String> axisIndexToAxis,
			List<? extends IAxisCoordinatesDictionary> axisIndexToValueIndex) {
		this.axisIndexToAxis = ImmutableList.copyOf(axisIndexToAxis);

		// The input may mutate, when new axis are indexes.
		this.axisIndexToCoordinateIndex = axisIndexToValueIndex;
	}

	@Override
	public NavigableSet<String> axes() {
		return new TreeSet<>(axisIndexToAxis);
	}

	@Override
	public String indexToAxis(int axisIndex) {
		if (axisIndex < 0) {
			throw new IllegalArgumentException("axisIndex has to be positive: " + axisIndex);
		}
		return axisIndexToAxis.get(axisIndex);
	}

	@Override
	public int getAxisIndex(String axis) {
		return axisIndexToAxis.indexOf(axis);
	}

	@Override
	public Object dereferenceCoordinate(int axisIndex, long coordinateIndex) {
		if (axisIndex < 0) {
			throw new IllegalArgumentException("axisIndex has to be positive: " + axisIndex);
		}
		return axisIndexToCoordinateIndex.get(axisIndex).getCoordinate(coordinateIndex);
	}

	@Override
	public long getCoordinateRef(int axisIndex, Object coordinate) {
		if (axisIndex < 0) {
			throw new IllegalArgumentException("axisIndex has to be positive: " + axisIndex);
		}
		return axisIndexToCoordinateIndex.get(axisIndex).getCoordinateIndex(coordinate);
	}

}
