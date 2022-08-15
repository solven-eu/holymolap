package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;

import com.google.common.primitives.Ints;

import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;
import eu.solven.holymolap.sink.AxisCoordinatesDictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class DataHolder implements IDataHolder {

	protected final int nbRow;
	protected final List<List<? extends RoaringBitmap>> axisIndexToCoordinateRefToRows;
	protected final List<? extends IntList> axisIndexToRowToInts;
	protected final List<? extends RoaringBitmap> axisIndexToRows;

	public DataHolder(int nbRow,
			List<? extends List<? extends RoaringBitmap>> keyIndexToValueIndexToBitmap,
			List<? extends IntList> axisIndexToRowToInts,
			List<? extends RoaringBitmap> axisIndexToRows) {
		this.nbRow = nbRow;
		this.axisIndexToCoordinateRefToRows = new ArrayList<>(keyIndexToValueIndexToBitmap);
		this.axisIndexToRowToInts = axisIndexToRowToInts;

		this.axisIndexToRows = axisIndexToRows;
	}

	@Override
	public long getAxisCardinality(int axisIndex) {
		List<? extends RoaringBitmap> coordinateRefToRows = axisIndexToCoordinateRefToRows.get(axisIndex);

		if (coordinateRefToRows == null) {
			IntList rowIndexToInt = axisIndexToRowToInts.get(axisIndex);

			if (rowIndexToInt == null) {
				throw new RuntimeException("We can not index the axisIndex: " + axisIndex);
			} else {
				synchronized (axisIndexToCoordinateRefToRows) {
					coordinateRefToRows = axisIndexToCoordinateRefToRows.get(axisIndex);
					if (coordinateRefToRows == null) {
						List<RoaringBitmap> newCoordinateRefToBitmap = new ArrayList<>();
						coordinateRefToRows = newCoordinateRefToBitmap;

						IAxisCoordinatesDictionary axisCoordinatesDictionary = new AxisCoordinatesDictionary();
						for (int rowWithValue : axisIndexToRows.get(axisIndex)) {
							int currentValue = rowIndexToInt.getInt(rowWithValue);

							long currentValueIndex = axisCoordinatesDictionary.getCoordinateIndex(currentValue);
							if (currentValueIndex == IAxisCoordinatesDictionary.NOT_INDEXED) {
								// This is the first encounter of this value
								currentValueIndex = axisCoordinatesDictionary.mapCoordinateIndex(currentValue);

								assert currentValueIndex == coordinateRefToRows.size();
								newCoordinateRefToBitmap.add(new RoaringBitmap());
							}

							newCoordinateRefToBitmap.get(Ints.checkedCast(currentValueIndex)).add(rowWithValue);
						}

						axisIndexToCoordinateRefToRows.set(axisIndex, coordinateRefToRows);
					}
				}
			}
		}

		return coordinateRefToRows.size();
	}

	@Override
	public RoaringBitmap getCoordinateToBitmap(int axisIndex, long coordinateRef) {
		if (axisIndex < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		} else if (coordinateRef < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		}

		List<? extends RoaringBitmap> valueIndexToBitmap = axisIndexToCoordinateRefToRows.get(axisIndex);

		if (valueIndexToBitmap != null) {
			return valueIndexToBitmap.get(Ints.checkedCast(coordinateRef));
		}

		IntList rowIndexToInt = axisIndexToRowToInts.get(axisIndex);
		if (rowIndexToInt != null) {
			if (rowIndexToInt instanceof IntArrayList) {
				return RoaringBitmap.bitmapOf(((IntArrayList) rowIndexToInt).elements());
			} else {
				return RoaringBitmap.bitmapOf(rowIndexToInt.toIntArray());
			}
		}

		throw new IllegalArgumentException("We can not index the axisIndex: " + axisIndex);
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (List<? extends RoaringBitmap> bitmapList : axisIndexToCoordinateRefToRows) {
			for (RoaringBitmap bitmap : bitmapList) {
				sizeInBytes += bitmap.getSizeInBytes();
			}
		}

		return sizeInBytes;
	}
}
