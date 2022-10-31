package eu.solven.holymolap.immutable.table;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.immutable.axis.IScannableAxisSmallColumn;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

public class HolyDictionarizedTable implements IHolyDictionarizedTable {
	// private static final Logger LOGGER = LoggerFactory.getLogger(HolyFlatDictionarizedTable.class);

	final int nbRows;
	protected final List<? extends IScannableAxisSmallColumn> axisIndexToColumn;

	public HolyDictionarizedTable(int nbRow, List<? extends IScannableAxisSmallColumn> axisIndexToColumn) {
		this.nbRows = nbRow;
		this.axisIndexToColumn = ImmutableList.copyOf(axisIndexToColumn);
	}

	@Override
	public long getSizeInBytes() {
		return axisIndexToColumn.stream().mapToLong(IHasMemoryFootprint::getSizeInBytes).sum();
	}

	@Override
	public long[] getCellCoordinates(long cellIndex, int... axesIndexes) {
		long[] cellCoordinates = new long[axesIndexes.length];

		for (int axisIndexIndex = 0; axisIndexIndex < axesIndexes.length; axisIndexIndex++) {
			int axisIndex = axesIndexes[axisIndexIndex];

			if (axisIndex < 0) {
				throw new IllegalArgumentException("No cell as axisIndexIndex=" + axisIndexIndex + " is unknown");
			}

			int coordinate = axisIndexToColumn.get(axisIndex).getCoordinateRef(cellIndex);

			cellCoordinates[axisIndexIndex] = coordinate;
		}

		return cellCoordinates;
	}

	@Override
	public RoaringBitmap getCoordinateToRows(int[] axesIndexes, long[] valuesRefs) {
		if (axesIndexes.length != valuesRefs.length) {
			throw new IllegalArgumentException(
					"Incompatible .length: " + axesIndexes.length + " vs " + valuesRefs.length);
		}

		if (axesIndexes.length == 0) {
			return RoaringBitmap.bitmapOfRange(0L, (long) nbRows);
		}

		List<RoaringBitmap> coordinateBitmap = new ArrayList<>(axesIndexes.length);

		for (int i = 0; i < axesIndexes.length; i++) {
			int axisIndex = axesIndexes[i];
			coordinateBitmap.add(axisIndexToColumn.get(axisIndex).getCoordinateBitmap(valuesRefs[i]));
		}

		return RoaringBitmap.and(coordinateBitmap.iterator(), 0L, (long) nbRows);
	}

}
