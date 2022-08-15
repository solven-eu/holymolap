package eu.solven.holymolap.cube.index;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.roaringbitmap.RoaringBitmap;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.holymolap.IDataHolder;
import eu.solven.holymolap.axes.AxisWithCoordinates;
import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

@ManagedResource
public class HolyCellSet implements ILazyHolyCellSet {

	protected final long nbRows;
	protected final List<? extends String> axisIndexToAxis;

	protected final List<LongList> axisToRowToValueIndex;
	protected final List<LongList> axesBeingIndexed;

	protected final IHasAxesWithCoordinates axesWithCoordinates;

	protected final IDataHolder dataHolder;

	public HolyCellSet(int nbRows,
			List<? extends String> keyIndexToKey,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesIndex,
			IDataHolder dataHolder) {
		this.nbRows = nbRows;
		this.axisIndexToAxis = keyIndexToKey;

		int nbKeys = keyIndexToKey.size();

		this.axisToRowToValueIndex = Arrays.asList(new LongList[nbKeys]);
		this.axesBeingIndexed = Arrays.asList(new LongList[nbKeys]);

		this.axesWithCoordinates = new AxisWithCoordinates(axisIndexToAxis, axisIndexToAxisCoordinatesIndex);
		this.dataHolder = dataHolder;
	}

	@Override
	public int getAxisIndex(String axis) {
		return axesWithCoordinates.getAxisIndex(axis);
	}

	@Override
	public ListenableFuture<?> startIndexing(int keyIndex) {
		if (axisToRowToValueIndex.get(keyIndex) == null) {
			synchronized (axisToRowToValueIndex) {
				// Check if there is no index yet, and it is not being
				// computed
				if (axisToRowToValueIndex.get(keyIndex) == null && axesBeingIndexed.get(keyIndex) == null) {
					long[] rawIndex = new long[Ints.checkedCast(nbRows)];
					Arrays.fill(rawIndex, NOT_INDEXED);

					LongList valuesIndex = new LongArrayList(rawIndex);
					axesBeingIndexed.set(keyIndex, valuesIndex);

					return indexBuilder().buildIndex(keyIndex,
							dataHolder,
							valuesIndex,
							indexedKey -> onIndexingCompleted(indexedKey));
				}
			}
		}

		return Futures.immediateVoidFuture();
	}

	protected PositionIndexBuilder indexBuilder() {
		return new PositionIndexBuilder();
	}

	protected void onIndexingCompleted(int indexedKey) {
		synchronized (axisToRowToValueIndex) {
			// This key has finished being indexed
			LongList index = axesBeingIndexed.get(indexedKey);

			// Put in keyToRowToValueIndex BEFORE removing from
			// keysBeingIndexed, else startIndexing could believe this key
			// is not indexed
			axisToRowToValueIndex.set(indexedKey, index);
			axesBeingIndexed.set(indexedKey, null);
		}
	}

	@ManagedAttribute
	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (LongList primitives : axisToRowToValueIndex) {
			if (primitives instanceof LongArrayList) {
				sizeInBytes += 4 * ((LongArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 4 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public long[] getCellCoordinatesRef(long cellIndex, int... axesIndexes) {
		int sliceSize = axesIndexes.length;

		long[] coordinates = new long[sliceSize];

		for (int i = 0; i < sliceSize; i++) {
			int axisIndex = axesIndexes[i];
			if (axisIndex < 0) {
				throw new IllegalArgumentException("axisIndexes has to be positive");
			} else {
				LongList rowToValueIndex = axisToRowToValueIndex.get(axisIndex);

				if (rowToValueIndex == null) {
					startIndexing(axisIndex);

					// TODO: a proper notify mechanism
					while (axisToRowToValueIndex.get(axisIndex) == null) {
						waitForIndexation(axisIndex);
					}
					rowToValueIndex = axisToRowToValueIndex.get(axisIndex);
				}

				coordinates[i] = rowToValueIndex.getLong(Ints.checkedCast(axisIndex));
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

	@Override
	public RoaringBitmap getCoordinateToBitmap(int axisIndex, long coordinateRef) {
		return dataHolder.getCoordinateToBitmap(axisIndex, coordinateRef);
	}

	@Override
	public Object dereferenceCoordinate(int axisIndex, long coordinateIndex) {
		return axesWithCoordinates.dereferenceCoordinate(axisIndex, coordinateIndex);
	}

	@Override
	public long getCoordinateRef(int axisIndex, Object coordinate) {
		return axesWithCoordinates.getCoordinateRef(axisIndex, coordinate);
	}

	// Slow
	// @Override
	// public Object getValueAtRow(String key, long row) {
	// int keyIndex = getKeyIndex(key);
	//
	// // TODO: check keyBitmap
	//
	// // Slow
	// for (int valueIndex = 0; valueIndex < dataHolder.getKeyCardinality(keyIndex); valueIndex++) {
	// if (dataHolder.getValueIndexToBitmap(keyIndex, valueIndex).contains(Ints.checkedCast(row))) {
	//
	// IKeyValuesIndex keyValuesIndex = keyIndexToValueIndex.get(keyIndex);
	// return keyValuesIndex.getValue(valueIndex);
	// }
	// }
	//
	// // We found no matching value
	// return null;
	// }

	// @Override
	// public List<?> getValuesForKey(String key) {
	// return keyIndexToValueIndex.get(getKeyIndex(key)).values();
	// }

	@Override
	public NavigableSet<String> axes() {
		return new TreeSet<>(this.axisIndexToAxis);
	}

	@Override
	public String indexToAxis(int keyIndex) {
		return axisIndexToAxis.get(keyIndex);
	}

}
