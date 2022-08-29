package eu.solven.holymolap.cube.cellset;

import java.util.List;
import java.util.NavigableSet;

import org.roaringbitmap.RoaringBitmap;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.IHolyDictionarizedTable;
import eu.solven.holymolap.axes.AxisWithCoordinates;
import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;

/**
 * This couple an {@link IHasAxesWithCoordinates} (which dictionarized {@link Object} coordinates to {@link Long}
 * coordinateRefs) with an {@link IHolyDictionarizedTable} (which knows only about dictionarized table).
 * 
 * @author Benoit Lacelle
 *
 */
@ManagedResource
public class HolyCellMultiSet implements IHolyCellMultiSet {
	protected final IHasAxesWithCoordinates axesWithCoordinates;
	protected final IHolyDictionarizedTable dictionarizedTable;

	@Deprecated
	public HolyCellMultiSet(List<? extends String> axisIndexToAxis,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesDictionary,
			IHolyDictionarizedTable dictionarizedTable) {
		this(new AxisWithCoordinates(axisIndexToAxis, axisIndexToAxisCoordinatesDictionary), dictionarizedTable);
	}

	public HolyCellMultiSet(IHasAxesWithCoordinates axesWithCoordinates, IHolyDictionarizedTable dictionarizedTable) {
		this.axesWithCoordinates = axesWithCoordinates;
		this.dictionarizedTable = dictionarizedTable;
	}

	@Override
	public int getAxisIndex(String axis) {
		return axesWithCoordinates.getAxisIndex(axis);
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		sizeInBytes += axesWithCoordinates.getSizeInBytes();
		sizeInBytes += dictionarizedTable.getSizeInBytes();

		return sizeInBytes;
	}

	@Override
	public long[] getCellCoordinates(long cellIndex, int... axesIndexes) {
		return dictionarizedTable.getCellCoordinates(cellIndex, axesIndexes);
	}

	@Override
	public RoaringBitmap getCoordinateToCells(int axisIndex, long coordinateRef) {
		return dictionarizedTable.getCoordinateToRows(axisIndex, coordinateRef);
	}

	@Override
	public Object dereferenceCoordinate(int axisIndex, long coordinateIndex) {
		return axesWithCoordinates.dereferenceCoordinate(axisIndex, coordinateIndex);
	}

	@Override
	public long getCoordinateRef(int axisIndex, Object coordinate) {
		return axesWithCoordinates.getCoordinateRef(axisIndex, coordinate);
	}

	@Override
	public NavigableSet<String> axes() {
		return axesWithCoordinates.axes();
	}

	@Override
	public List<String> getAxes() {
		return axesWithCoordinates.getAxes();
	}

	@Override
	public String indexToAxis(int axisIndex) {
		return axesWithCoordinates.indexToAxis(axisIndex);
	}

}
