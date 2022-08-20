package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.cellset.PositionIndexBuilder;
import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.sink.AxisCoordinatesDictionary;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

public class HolyDictionarizedTable implements IHolyDictionarizedTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(HolyDictionarizedTable.class);

	protected final int nbRows;
	protected final List<List<? extends RoaringBitmap>> axisIndexToCoordinateRefToRows;
	// protected final List<? extends IntList> axisIndexToRowToInts;
	protected final List<? extends RoaringBitmap> axisIndexToRows;

	protected final List<LongList> axisToRowToCoordinateRef;
	protected final List<LongList> axesBeingIndexed;

	public HolyDictionarizedTable(int nbRow,
			List<? extends List<? extends RoaringBitmap>> axisIndexToCoordinateRefToCells,
			// List<? extends IntList> axisIndexToRowToInts,
			List<? extends RoaringBitmap> axisIndexToRows) {
		this.nbRows = nbRow;
		this.axisIndexToCoordinateRefToRows = new ArrayList<>(axisIndexToCoordinateRefToCells);
		// this.axisIndexToRowToInts = axisIndexToRowToInts;

		this.axisIndexToRows = axisIndexToRows;

		int nbAxes = axisIndexToRows.size();
		this.axisToRowToCoordinateRef = Arrays.asList(new LongList[nbAxes]);
		this.axesBeingIndexed = Arrays.asList(new LongList[nbAxes]);
	}

	@Override
	public long getAxisCardinality(int axisIndex) {
		List<? extends RoaringBitmap> coordinateRefToRows = axisIndexToCoordinateRefToRows.get(axisIndex);

		// if (coordinateRefToRows == null) {
		// IntList rowIndexToInt = axisIndexToRowToInts.get(axisIndex);
		//
		// if (rowIndexToInt != null) {
		// synchronized (axisIndexToCoordinateRefToRows) {
		// coordinateRefToRows = axisIndexToCoordinateRefToRows.get(axisIndex);
		// if (coordinateRefToRows == null) {
		// List<RoaringBitmap> newCoordinateRefToBitmap = new ArrayList<>();
		// coordinateRefToRows = newCoordinateRefToBitmap;
		//
		// IAxisCoordinatesDictionary axisCoordinatesDictionary = new AxisCoordinatesDictionary();
		// for (int rowWithValue : axisIndexToRows.get(axisIndex)) {
		// int currentValue = rowIndexToInt.getInt(rowWithValue);
		//
		// long currentValueIndex = axisCoordinatesDictionary.getCoordinateIndex(currentValue);
		// if (currentValueIndex == IAxisCoordinatesDictionary.NOT_INDEXED) {
		// // This is the first encounter of this value
		// currentValueIndex = axisCoordinatesDictionary.mapCoordinateIndex(currentValue);
		//
		// assert currentValueIndex == coordinateRefToRows.size();
		// newCoordinateRefToBitmap.add(new RoaringBitmap());
		// }
		//
		// newCoordinateRefToBitmap.get(Ints.checkedCast(currentValueIndex)).add(rowWithValue);
		// }
		//
		// axisIndexToCoordinateRefToRows.set(axisIndex, coordinateRefToRows);
		// }
		// }
		// }
		// }

		if (coordinateRefToRows == null) {
			throw new IllegalArgumentException("We can not index the axisIndex: " + axisIndex);
		}

		return coordinateRefToRows.size();
	}

	@Override
	public RoaringBitmap getCoordinateToRows(int axisIndex, long coordinateRef) {
		if (axisIndex < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		} else if (coordinateRef < 0) {
			return HolyExceptionManagement.immutableEmptyBitmap();
		}

		List<? extends RoaringBitmap> valueIndexToBitmap = axisIndexToCoordinateRefToRows.get(axisIndex);

		if (valueIndexToBitmap != null) {
			return valueIndexToBitmap.get(Ints.checkedCast(coordinateRef));
		}

		// IntList rowIndexToInt = axisIndexToRowToInts.get(axisIndex);
		// if (rowIndexToInt != null) {
		// if (rowIndexToInt instanceof IntArrayList) {
		// return RoaringBitmap.bitmapOf(((IntArrayList) rowIndexToInt).elements());
		// } else {
		// return RoaringBitmap.bitmapOf(rowIndexToInt.toIntArray());
		// }
		// }

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

		for (LongList primitives : axisToRowToCoordinateRef) {
			if (primitives instanceof LongArrayList) {
				sizeInBytes += 4 * ((LongArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 4 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public long[] getCellCoordinates(long cellIndex, int[] axesIndexes) {
		int sliceDimensionality = axesIndexes.length;

		long[] coordinates = new long[sliceDimensionality];

		for (int i = 0; i < sliceDimensionality; i++) {
			int axisIndex = axesIndexes[i];
			if (axisIndex < 0) {
				coordinates[i] = IHasAxesWithCoordinates.NOT_INDEXED;
			} else {
				LongList rowToCoordinateRef = axisToRowToCoordinateRef.get(axisIndex);

				if (rowToCoordinateRef == null) {
					startIndexing(axisIndex);

					// TODO: a proper notify mechanism
					while (axisToRowToCoordinateRef.get(axisIndex) == null) {
						waitForIndexation(axisIndex);
					}
					rowToCoordinateRef = axisToRowToCoordinateRef.get(axisIndex);
				}

				coordinates[i] = rowToCoordinateRef.getLong(Ints.checkedCast(cellIndex));
			}
		}

		return coordinates;
	}

	protected void waitForIndexation(int axisIndex) {
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	// @Override
	public ListenableFuture<?> startIndexing(int axisIndex) {
		if (axisToRowToCoordinateRef.get(axisIndex) == null) {
			synchronized (axisToRowToCoordinateRef) {
				// Check if there is no index yet, and it is not being
				// computed
				if (axisToRowToCoordinateRef.get(axisIndex) == null && axesBeingIndexed.get(axisIndex) == null) {
					long[] rowToCoordinateRefAsArray = new long[Ints.checkedCast(nbRows)];
					Arrays.fill(rowToCoordinateRefAsArray, IHasAxesWithCoordinates.NOT_INDEXED);

					LongList rowToCoordinateRef = new LongArrayList(rowToCoordinateRefAsArray);
					axesBeingIndexed.set(axisIndex, rowToCoordinateRef);

					return indexBuilder()
							.buildIndex(axisIndex, this, rowToCoordinateRef, () -> onIndexingCompleted(axisIndex));
				}
			}
		}

		return Futures.immediateVoidFuture();
	}

	protected PositionIndexBuilder indexBuilder() {
		return new PositionIndexBuilder();
	}

	protected void onIndexingCompleted(int axisIndex) {
		synchronized (axisToRowToCoordinateRef) {
			// This key has finished being indexed
			LongList rowToCoordinateRef = axesBeingIndexed.get(axisIndex);

			if (rowToCoordinateRef == null) {
				LOGGER.warn("{} is said being dictionarized while not dictionarization is pending", axisIndex);
				return;
			}

			// Put in keyToRowToValueIndex BEFORE removing from
			// keysBeingIndexed, else startIndexing could believe this key
			// is not indexed
			axisToRowToCoordinateRef.set(axisIndex, rowToCoordinateRef);
			axesBeingIndexed.set(axisIndex, null);
		}
	}
}
