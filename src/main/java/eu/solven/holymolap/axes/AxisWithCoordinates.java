package eu.solven.holymolap.axes;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.immutable.IAxisCoordinatesDictionary;

public class AxisWithCoordinates implements IHasAxesWithCoordinates {
	private static final Logger LOGGER = LoggerFactory.getLogger(AxisWithCoordinates.class);

	protected final List<? extends String> axisIndexToAxis;
	protected final List<? extends IAxisCoordinatesDictionary> axisIndexToCoordinateIndex;

	public AxisWithCoordinates(List<? extends String> axisIndexToAxis,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesDictionary) {
		this.axisIndexToAxis = ImmutableList.copyOf(axisIndexToAxis);

		// The input may mutate, when new axis are indexes.
		this.axisIndexToCoordinateIndex = axisIndexToAxisCoordinatesDictionary;
	}

	@Override
	public NavigableSet<String> axes() {
		return Collections.unmodifiableNavigableSet(new TreeSet<>(axisIndexToAxis));
	}

	@Override
	public List<String> getAxes() {
		return Collections.unmodifiableList(axisIndexToAxis);
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
		return axisIndexToCoordinateIndex.get(axisIndex).getCoordinateRef(coordinate);
	}

	@Override
	public long getSizeInBytes() {
		LOGGER.warn("TODO");
		return 0;
	}

	@Override
	public long getCardinality(int axisIndex) {
		return axisIndexToCoordinateIndex.get(axisIndex).coordinates().size();
	}

}
