package eu.solven.holymolap.cube.cellset;

import java.util.List;
import java.util.NavigableSet;

import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.axes.AxisWithCoordinates;
import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.immutable.IAxisCoordinatesDictionary;
import eu.solven.holymolap.cube.table.IHolyDictionarizedTable;

/**
 * This couple an {@link IHasAxesWithCoordinates} (which dictionarized {@link Object} coordinates to {@link Long}
 * coordinateRefs) with an {@link IHolyDictionarizedTable} (which knows only about dictionarized table).
 * 
 * @author Benoit Lacelle
 *
 */
@ManagedResource
public class HolyBitmapCellMultiSet implements IHolyCellMultiSet {
	protected final IHasAxesWithCoordinates axesWithCoordinates;
	protected final IHolyDictionarizedTable dictionarizedTable;

	@Deprecated
	public HolyBitmapCellMultiSet(List<? extends String> axisIndexToAxis,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesDictionary,
			IHolyDictionarizedTable dictionarizedTable) {
		this(new AxisWithCoordinates(axisIndexToAxis, axisIndexToAxisCoordinatesDictionary), dictionarizedTable);
	}

	public HolyBitmapCellMultiSet(IHasAxesWithCoordinates axesWithCoordinates,
			IHolyDictionarizedTable dictionarizedTable) {
		this.axesWithCoordinates = axesWithCoordinates;
		this.dictionarizedTable = dictionarizedTable;
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		sizeInBytes += axesWithCoordinates.getSizeInBytes();
		sizeInBytes += dictionarizedTable.getSizeInBytes();

		return sizeInBytes;
	}

	@Override
	public IHolyDictionarizedTable getTable() {
		return dictionarizedTable;
	}

	@Override
	public IHasAxesWithCoordinates getAxesWithCoordinates() {
		return axesWithCoordinates;
	}

}
