package eu.solven.holymolap.aggregate;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;

public class CoordinatesRefs implements ICoordinatesRefs {
	final IHasAxesWithCoordinates axesWithCoordinates;
	final int[] axesIndexes;
	final long[] coordinatesRef;

	public CoordinatesRefs(IHasAxesWithCoordinates axesWithCoordinates, int[] axesIndexes, long[] coordinatesRef) {
		this.axesWithCoordinates = axesWithCoordinates;
		this.axesIndexes = axesIndexes;
		this.coordinatesRef = coordinatesRef;

		if (axesIndexes.length != coordinatesRef.length) {
			throw new IllegalArgumentException(axesIndexes.length + " != " + coordinatesRef.length);
		}
	}

	public IHasAxesWithCoordinates getAxesWithCoordinates() {
		return axesWithCoordinates;
	}

	public int[] getAxesIndexes() {
		return axesIndexes;
	}

	public long[] getCoordinatesRef() {
		return coordinatesRef;
	}

	@Override
	public String toString() {
		return IntStream.range(0, coordinatesRef.length).mapToObj(i -> {
			int axisIndex = axesIndexes[i];
			return axesWithCoordinates.getAxes().get(axisIndex) + "->'"
					+ axesWithCoordinates.dereferenceCoordinate(axisIndex, coordinatesRef[i])
					+ "'";
		}).collect(Collectors.joining(", "));
	}

}
