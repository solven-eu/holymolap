package eu.solven.holymolap.cube.mutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.HolyDictionarizedTable;
import eu.solven.holymolap.IHolyDictionarizedTable;
import eu.solven.holymolap.axes.AxisWithCoordinates;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.aggregates.HolyAggregateTable;
import eu.solven.holymolap.cube.aggregates.IHolyAggregateTable;
import eu.solven.holymolap.cube.aggregates.IHolyAggregateTableDefinition;
import eu.solven.holymolap.cube.cellset.HolyCellMultiSet;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.cube.immutable.IScannableDoubleAggregatesColumn;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.sink.AxisCoordinatesDictionary;
import eu.solven.holymolap.sink.IAxisCoordinatesDictionary;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.stable.v1.IAggregatedAxis;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * Enables loading data into an {@link IHolyCube}. It is specialized for loading. It may be queried, but with a chance
 * of inconsistencies as some view may return holding partial rows.
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableHolyCube implements IMutableHolyCube {
	private static final Logger LOGGER = LoggerFactory.getLogger(MutableHolyCube.class);

	final IHolyAggregateTableDefinition aggregations;
	final Map<IAggregatedAxis, IMutableDoubleAggregatesColumn> aggregationToColumn;
	// The Set of axes with at least one cell
	final Set<String> axesNames;

	final List<String> orderedAxes;
	final Map<String, IMutableAxisColumn> axisToColumn;

	// A cell is identified by a tuple of coordinateIndexes along axes
	// cellToRow can be recomputed from axisToColumn, but it is necessary to quickly identify to which row an input has
	// to be aggregated into.
	final Object2IntMap<IntList> cellToRow;

	final AtomicLong brokenRows = new AtomicLong();

	final AtomicBoolean closed = new AtomicBoolean();

	protected MutableHolyCube(IHolyAggregateTableDefinition aggregations,
			Map<IAggregatedAxis, IMutableDoubleAggregatesColumn> aggregationToColumn,
			List<String> orderedAxis,
			Map<String, IMutableAxisColumn> axisToColumn,
			Object2IntMap<IntList> cellToRow) {
		this.aggregations = aggregations;
		this.aggregationToColumn = aggregationToColumn;
		this.axesNames = aggregationToColumn.keySet().stream().map(aa -> aa.getAxis()).collect(Collectors.toSet());

		this.orderedAxes = orderedAxis;
		this.axisToColumn = axisToColumn;
		if (orderedAxis.size() != axisToColumn.size()) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		} else if (!orderedAxis.stream().distinct().collect(Collectors.toSet()).equals(axisToColumn.keySet())) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		}

		this.cellToRow = cellToRow;
		if (cellToRow.defaultReturnValue() != IMutableAxisDictionary.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + cellToRow.defaultReturnValue());
		}
	}

	/**
	 * The {@link IAggregatedAxis} are provided from the beginning, as they will be
	 * 
	 * @param aggregations
	 */
	public MutableHolyCube(IHolyAggregateTableDefinition aggregations) {
		this(aggregations,
				prepareAggregationColumns(aggregations),
				prepareOrderedAxes(),
				new ConcurrentHashMap<>(),
				prepareCellToRow());
	}

	private static Object2IntOpenHashMap<IntList> prepareCellToRow() {
		Object2IntOpenHashMap<IntList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	private static List<String> prepareOrderedAxes() {
		// We chose a thread-safe collection not to synchronize any read from it.
		// We expect a low number of writes, hence COpyOnWrite is OK.
		return new CopyOnWriteArrayList<String>();
	}

	private static Map<IAggregatedAxis, IMutableDoubleAggregatesColumn> prepareAggregationColumns(
			IHolyAggregateTableDefinition aggregations) {
		OperatorFactory operatorFactory = new OperatorFactory();

		return aggregations.aggregations()
				.stream()
				.collect(Collectors.toMap(a -> a.asAggregatedAxis(),
						a -> provisionAggregateColumn(operatorFactory, a.asAggregatedAxis())));
	}

	protected static MutableAggregatesColumn provisionAggregateColumn(OperatorFactory operatorFactory,
			IAggregatedAxis a) {
		return new MutableAggregatesColumn(operatorFactory.getDoubleBinaryOperator(a.getOperator()));
	}

	// @Override
	// public IHasAxesWithCoordinates getAxes() {
	// return null;
	// }

	/**
	 * 
	 * @param row
	 *            a row, to be contributed to either a new, or an existing cell.
	 */
	@Override
	public void acceptRowToCell(Map<String, ?> row) {
		Map<String, Object> groupBy = new HashMap<>();
		Map<String, Object> aggregated = new HashMap<>();

		row.forEach((axis, coordinate) -> {
			if (axesNames.contains(axis)) {
				aggregated.put(axis, coordinate);
			} else {
				groupBy.put(axis, coordinate);
			}
		});

		acceptRowToCell(aggregated, groupBy);
	}

	@Override
	public void acceptRowToCell(Map<String, ?> aggregated, Map<String, ?> groupBy) {
		SetView<String> invalidGroupBys = Sets.intersection(axesNames, groupBy.keySet());
		if (!invalidGroupBys.isEmpty()) {
			throw new IllegalArgumentException("Can not groupBy aggregated keys: " + invalidGroupBys);
		}

		if (groupBy.size() > orderedAxes.size()) {
			// This row introduces a new axes
			Set<String> additionalAxes = new LinkedHashSet<>(groupBy.keySet());
			additionalAxes.removeAll(axisToColumn.keySet());

			orderedAxes.addAll(additionalAxes);
			additionalAxes.forEach(axis -> axisToColumn.putIfAbsent(axis, new MutableAxisColumn()));
		}

		// Append the coordinates for the known axes
		IntList cellCoordinates = indexGroupBy(groupBy);

		int cellIndex;
		int newCellIndex;
		synchronized (cellToRow) {
			int localCellIndex = cellToRow.getInt(cellCoordinates);
			if (localCellIndex >= 0) {
				cellIndex = localCellIndex;
				newCellIndex = IMutableAxisDictionary.NO_COORDINATE_INDEX;
			} else {
				newCellIndex = cellToRow.size();
				cellIndex = newCellIndex;
				cellToRow.put(cellCoordinates, newCellIndex);
			}
		}

		if (newCellIndex >= 0) {
			// Register this new cell
			for (int axisIndex = 0; axisIndex < cellCoordinates.size(); axisIndex++) {
				String axis = orderedAxes.get(axisIndex);
				IMutableAxisColumn column = axisToColumn.get(axis);
				int cellAxisCoordinate = cellCoordinates.getInt(axisIndex);
				column.appendCoordinateIndex(cellAxisCoordinate);
			}
		}

		aggregationToColumn.forEach((aggregatedAxis, column) -> {
			String axis = aggregatedAxis.getAxis();
			Object contribution = aggregated.get(axis);

			if (contribution instanceof Number) {
				column.aggregateRow(cellIndex, ((Number) contribution).doubleValue());
			} else if (contribution != null) {
				brokenRows.incrementAndGet();
			}
		});
	}

	@Override
	public void acceptRowToCell(Stream<? extends IHolyCubeRecord> toAdd) {

		int nbKeys = context.keyIndexToKey().size();

		List<RoaringBitmap> axisIndexToBitmap;
		{
			axisIndexToBitmap = Arrays.asList(new RoaringBitmap[nbKeys]);
			for (int i = 0; i < nbKeys; i++) {
				axisIndexToBitmap.set(i, new RoaringBitmap());
			}
		}

		// List<DoubleList> axisIndexToDoubles = Arrays.asList(new DoubleList[nbKeys]);
		// List<IntList> axisIndexToInts = Arrays.asList(new IntList[nbKeys]);

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
				// Moving to the next entry may have made appear a new key
				throw new UnsupportedOperationException("TODO");
			}

			for (int keyIndex : next.doubleIndexes()) {
				axisIndexToBitmap.get(keyIndex).add(currentRowIndex);

				double value = next.getDouble(keyIndex);

				contributeDoubleToRow(value, keyIndex, axisIndexToDoubles, context.expectedNbRows(), currentRowIndex);
			}

			for (int keyIndex : next.intIndexes()) {
				axisIndexToBitmap.get(keyIndex).add(currentRowIndex);

				int value = next.getInt(keyIndex);

				contributeIntToRow(value, keyIndex, axisIndexToInts, context.expectedNbRows(), currentRowIndex);
			}

			for (int axisIndex : next.objectIndexes()) {
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
					// We consider numbers have generally high cardinality: never
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
						axisIndexToValueIndexToBitmap
				// ,
				// axisIndexToInts
				),
				new HolyAggregateTable(axisIndexToDoubles));
	}

	private HolyCellMultiSet makeCellset(int nbRows,
			List<? extends String> axisIndexToAxis,
			List<? extends RoaringBitmap> axisIndexToBitmap,
			List<? extends IAxisCoordinatesDictionary> axisIndexToAxisCoordinatesDictionary,
			List<? extends List<RoaringBitmap>> axisIndexToCoordinateRefToRows
	// ,
	// List<? extends IntList> axisIndexToRowToInts
	) {
		IHasAxesWithCoordinates axesWithCoordinates =
				new AxisWithCoordinates(axisIndexToAxis, axisIndexToAxisCoordinatesDictionary);
		return new HolyCellMultiSet(axesWithCoordinates,
				new HolyDictionarizedTable(nbRows, axisIndexToCoordinateRefToRows
				// , axisIndexToRowToInts
						, axisIndexToBitmap));
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

	protected IntList indexGroupBy(Map<String, ?> groupBy) {
		IntList cellCoordinates = new IntArrayList(groupBy.size());

		// Register the coordinate
		orderedAxes.stream().forEach(axis -> {
			Object coordinate = groupBy.get(axis);

			int coordinateIndex;
			if (coordinate == null || IMutableAxisDictionary.NO_COORDINATE == coordinate) {
				coordinateIndex = IMutableAxisDictionary.NO_COORDINATE_INDEX;
			} else {
				coordinateIndex = axisToColumn.get(axis).getCoordinateToIndex().getIndexMayAppend(coordinate);
			}

			cellCoordinates.add(coordinateIndex);
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		while (!cellCoordinates.isEmpty()
				&& cellCoordinates.getInt(cellCoordinates.size() - 1) == IMutableAxisDictionary.NO_COORDINATE_INDEX) {
			cellCoordinates.removeInt(cellCoordinates.size() - 1);
		}

		return cellCoordinates;
	}

	@Override
	public IHolyCube closeToHolyCube() {
		if (!closed.compareAndSet(false, true)) {
			throw new IllegalStateException("Was already closed");
		}

		int nbRows = cellToRow.size();
		IHasAxesWithCoordinates axesWithCoordinates;
		IHolyDictionarizedTable dictionarizedTable;
		IHolyCellMultiSet cellSet = new HolyCellMultiSet(axesWithCoordinates, dictionarizedTable);

		List<IScannableDoubleAggregatesColumn> aggregatedColumns = aggregations.aggregations()
				.stream()
				.map(def -> aggregationToColumn.get(def.asAggregatedAxis()))
				.map(mutableColumn -> {
					if (mutableColumn == null) {
						return null;
					} else {
						return mutableColumn.flush();
					}
				})
				.collect(Collectors.toList());

		IHolyAggregateTable aggregateTable = new HolyAggregateTable(aggregations, aggregatedColumns);
		return new HolyCube(nbRows, cellSet, aggregateTable);
	}

}
