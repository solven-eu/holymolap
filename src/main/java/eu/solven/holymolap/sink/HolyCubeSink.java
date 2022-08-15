package eu.solven.holymolap.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.DataHolder;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.aggregates.HolyAggregateTable;
import eu.solven.holymolap.cube.index.HolyCellSet;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class HolyCubeSink implements IHolySink {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCubeSink.class);

	@Override
	public IHolyCube sink(ISinkContext context, Stream<? extends IFastEntry> toAdd) {
		int nbKeys = context.keyIndexToKey().size();

		List<RoaringBitmap> axisIndexToBitmap;
		{
			axisIndexToBitmap = Arrays.asList(new RoaringBitmap[nbKeys]);
			for (int i = 0; i < nbKeys; i++) {
				axisIndexToBitmap.set(i, new RoaringBitmap());
			}
		}

		List<DoubleList> axisIndexToDoubles = Arrays.asList(new DoubleList[nbKeys]);
		List<IntList> axisIndexToInts = Arrays.asList(new IntList[nbKeys]);

		List<IAxisCoordinatesDictionary> axisIndexToCoordinatesIndex =
				Arrays.asList(new IAxisCoordinatesDictionary[nbKeys]);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<List<RoaringBitmap>> axisIndexToValueIndexToBitmap = (List) Arrays.asList(new List[nbKeys]);

		final Meter requests = new Meter();
		AtomicInteger rowIndex = new AtomicInteger(-1);

		toAdd.forEach(next -> {
			if (rowIndex.get() == Integer.MAX_VALUE) {
				throw new IllegalArgumentException(
						"We can not sink an Iterable with more than Integer.MAX_VALUE elements");
			}

			int currentRowIndex = rowIndex.incrementAndGet();

			requests.mark();
			if (requests.getCount() % 100000 == 100000 - 1) {
				LOGGER.debug("We sinked {} rows. Loading at {}rows/sec", requests.getCount(), requests.getMeanRate());
			}

			if (context.hasNewKey()) {
				// Moving to next entry may have make appear a new key
				throw new UnsupportedOperationException("TODO");
			}

			for (int keyIndex : next.doubleAxesIndexes()) {
				axisIndexToBitmap.get(keyIndex).add(currentRowIndex);

				double value = next.getDouble(keyIndex);

				contributeDoubleToRow(value, keyIndex, axisIndexToDoubles, context.expectedNbRows(), currentRowIndex);
			}

			for (int keyIndex : next.intAxesIndexes()) {
				axisIndexToBitmap.get(keyIndex).add(currentRowIndex);

				int value = next.getInt(keyIndex);

				contributeIntToRow(value, keyIndex, axisIndexToInts, context.expectedNbRows(), currentRowIndex);
			}

			for (int axisIndex : next.objectAxesIndexes()) {
				if (axisIndex >= axisIndexToBitmap.size()) {
					throw new RuntimeException("The entry " + next
							+ " express index "
							+ axisIndex
							+ " while the context has "
							+ nbKeys
							+ " keys");
				}

				RoaringBitmap bitmap = axisIndexToBitmap.get(axisIndex);
				bitmap.add(currentRowIndex);

				Object value = next.getObject(axisIndex);

				if (value == null) {
					LOGGER.trace("Skip null value");
				} else if (value instanceof Number) {
					// We consider number have generally high cardinality: never
					// index them
					if (value instanceof Integer) {
						contributeIntToRow(((Number) value)
								.intValue(), axisIndex, axisIndexToInts, context.expectedNbRows(), currentRowIndex);
					} else {
						contributeDoubleToRow(((Number) value).doubleValue(),
								axisIndex,
								axisIndexToDoubles,
								context.expectedNbRows(),
								currentRowIndex);
					}
				} else {
					List<RoaringBitmap> valueToBitmap = axisIndexToValueIndexToBitmap.get(axisIndex);
					if (valueToBitmap == null) {
						valueToBitmap = new ArrayList<RoaringBitmap>();
						axisIndexToValueIndexToBitmap.set(axisIndex, valueToBitmap);
					}

					// We consider Object have low cardinality: always index
					// them
					IAxisCoordinatesDictionary axisCoordinatesIndex = axisIndexToCoordinatesIndex.get(axisIndex);
					if (axisCoordinatesIndex == null) {
						axisCoordinatesIndex = new AxisCoordinatesDictionary();
						axisIndexToCoordinatesIndex.set(axisIndex, axisCoordinatesIndex);
					}

					long valueIndex = axisCoordinatesIndex.mapCoordinateIndex(value);

					if (valueIndex == valueToBitmap.size()) {
						// This is the first time we encountered this value
						valueToBitmap.add(new RoaringBitmap());
					}

					RoaringBitmap valueBitmap = valueToBitmap.get(Ints.checkedCast(valueIndex));

					valueBitmap.add(currentRowIndex);
				}
			}
		});

		// NbRows is rowIndex + 1
		int nbRows = rowIndex.get() + 1;
		return new HolyCube(nbRows,
				makeCellset(nbRows,
						context.keyIndexToKey(),
						axisIndexToBitmap,
						axisIndexToCoordinatesIndex,
						axisIndexToValueIndexToBitmap,
						axisIndexToInts),
				new HolyAggregateTable(axisIndexToDoubles));
	}

	private HolyCellSet makeCellset(int nbRows,
			List<? extends String> axisIndexToAxis,
			List<? extends RoaringBitmap> axisIndexToBitmap,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesDictionary,
			List<? extends List<RoaringBitmap>> axisIndexToCoordinateRefToRows,
			List<? extends IntList> axisIndexToRowToInts) {
		return new HolyCellSet(nbRows,
				axisIndexToAxis,
				axisIndexToAxisCoordinatesDictionary,
				new DataHolder(nbRows, axisIndexToCoordinateRefToRows, axisIndexToRowToInts, axisIndexToBitmap));
	}

	protected void contributeDoubleToRow(double value,
			int keyIndex,
			List<DoubleList> keyIndexToDoubles,
			int expectedNbRows,
			int rowIndex) {
		DoubleList rowIndexToDouble = keyIndexToDoubles.get(keyIndex);

		if (rowIndexToDouble == null) {
			// NbRows is rowIndex + 1
			rowIndexToDouble = new DoubleArrayList(Math.max(expectedNbRows, rowIndex + 1));
			keyIndexToDoubles.set(keyIndex, rowIndexToDouble);
		}

		int currentSize = rowIndexToDouble.size();
		if (currentSize < rowIndex - 1) {
			// TODO: re-use a buffer array
			rowIndexToDouble.addElements(currentSize, new double[rowIndex - currentSize]);
		}

		rowIndexToDouble.add(value);
	}

	protected void contributeIntToRow(int value,
			int keyIndex,
			List<IntList> keyIndexToInts,
			int expectedNbRows,
			int rowIndex) {
		IntList rowIndexToInt = keyIndexToInts.get(keyIndex);

		if (rowIndexToInt == null) {
			// NbRows is rowIndex + 1
			rowIndexToInt = new IntArrayList(Math.max(expectedNbRows, rowIndex + 1));
			keyIndexToInts.set(keyIndex, rowIndexToInt);
		}

		int currentSize = rowIndexToInt.size();
		if (currentSize < rowIndex - 1) {
			// TODO: re-use a buffer array
			rowIndexToInt.addElements(currentSize, new int[rowIndex - currentSize]);
		}

		rowIndexToInt.add(value);
	}

}
