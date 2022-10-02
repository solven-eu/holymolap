package eu.solven.holymolap.mutable.cube;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.HolyBitmapCellMultiSet;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.immutable.axes.AxisWithCoordinates;
import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.immutable.axis.IScannableAxisSmallColumn;
import eu.solven.holymolap.immutable.axis.ImmutableAxisSmallColumn;
import eu.solven.holymolap.immutable.column.IScannableMeasureColumn;
import eu.solven.holymolap.immutable.dictionary.AxisCoordinatesDictionary;
import eu.solven.holymolap.immutable.dictionary.IAxisCoordinatesDictionary;
import eu.solven.holymolap.immutable.table.HolyDictionarizedTable;
import eu.solven.holymolap.immutable.table.IHolyDictionarizedTable;
import eu.solven.holymolap.measures.HolyMeasuresTable;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.measures.operator.IOperatorFactory;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallColumn;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionarySink;
import eu.solven.holymolap.mutable.axis.MutableAxisColumn;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableLongAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableLongAggregatesColumn;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
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

	final IHolyMeasuresDefinition aggregations;
	final Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn;

	// The Set of axes with at least one cell
	final Set<String> axesNames;
	final List<String> orderedAxes;
	final Map<String, IMutableAxisSmallColumn> axisToColumn;

	// A cell is identified by a tuple of coordinateIndexes along axes
	// cellToRow can be recomputed from axisToColumn, but it is necessary to quickly identify to which row an input has
	// to be aggregated into.
	// This can be rebuilt from (orderedAxes, axisToColumn)
	final Object2IntMap<IntList> cellToRow;

	final AtomicLong brokenRows = new AtomicLong();
	final AtomicBoolean closed = new AtomicBoolean();

	protected MutableHolyCube(IHolyMeasuresDefinition aggregations,
			Map<IMeasuredAxis, IMutableAggregatesColumn> aggregationToColumn,
			List<String> orderedAxis,
			Map<String, IMutableAxisSmallColumn> axisToColumn,
			Object2IntMap<IntList> cellToRow) {
		this.aggregations = aggregations;
		this.measureToColumn = aggregationToColumn;
		this.axesNames = aggregationToColumn.keySet().stream().map(aa -> aa.getAxis()).collect(Collectors.toSet());

		this.orderedAxes = orderedAxis;
		this.axisToColumn = axisToColumn;
		if (orderedAxis.size() != axisToColumn.size()) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		} else if (!orderedAxis.stream().distinct().collect(Collectors.toSet()).equals(axisToColumn.keySet())) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		}

		this.cellToRow = cellToRow;
		if (cellToRow.defaultReturnValue() != IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + cellToRow.defaultReturnValue());
		}
	}

	/**
	 * The {@link IMeasuredAxis} are provided as configuration
	 * 
	 * @param aggregations
	 */
	public MutableHolyCube(IHolyMeasuresDefinition aggregations) {
		this(aggregations,
				prepareAggregationColumns(aggregations),
				prepareOrderedAxes(),
				new ConcurrentHashMap<>(),
				prepareCellToRow());
	}

	private static Object2IntOpenHashMap<IntList> prepareCellToRow() {
		Object2IntOpenHashMap<IntList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	private static List<String> prepareOrderedAxes() {
		// We chose a thread-safe collection not to synchronize any read from it.
		// We expect a low number of writes, hence COpyOnWrite is OK.
		return new CopyOnWriteArrayList<String>();
	}

	private static Map<IMeasuredAxis, IMutableAggregatesColumn> prepareAggregationColumns(
			IHolyMeasuresDefinition aggregations) {
		OperatorFactory operatorFactory = new OperatorFactory();

		return aggregations.measures()
				.stream()
				.collect(Collectors.toMap(a -> a.asMeasuredAxis(),
						a -> provisionAggregateColumn(operatorFactory, a.asMeasuredAxis())));
	}

	protected static IMutableAggregatesColumn provisionAggregateColumn(IOperatorFactory operatorFactory,
			IMeasuredAxis measure) {
		IBinaryOperator binaryOperator = operatorFactory.getBinaryOperator(measure.getOperator());

		if (binaryOperator instanceof IDoubleBinaryOperator) {
			return new MutableDoubleAggregatesColumn((IDoubleBinaryOperator) binaryOperator);
		} else if (binaryOperator instanceof ILongBinaryOperator) {
			return new MutableLongAggregatesColumn((ILongBinaryOperator) binaryOperator);
		} else {
			return new MutableAggregatesColumn(binaryOperator);
		}
	}

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
		IntList cellCoordinates = cellToCoordinates(groupBy);

		int cellIndex = ensureCellRegistration(cellCoordinates);

		contributeToMeasures(aggregated, cellIndex);
	}

	public void acceptRowToCell(IHolyCubeRecord toAdd) {
		IntList cellCoordinates = cellToCoordinates(toAdd.getCellsetRecord());

		int cellIndex = ensureCellRegistration(cellCoordinates);

		contributeToMeasures(toAdd.getAggregateTableRecord(), cellIndex);
	}

	private void contributeToMeasures(Map<String, ?> aggregated, int cellIndex) {
		measureToColumn.forEach((aggregatedAxis, column) -> {
			String axis = aggregatedAxis.getAxis();
			Object contribution = aggregated.get(axis);

			if (contribution instanceof Number) {
				column.aggregateObject(cellIndex, contribution);
			} else if (contribution != null) {
				LOGGER.warn("Issue accepting contribution {} in {}", contribution, axis);
				brokenRows.incrementAndGet();
			}
		});
	}

	private void contributeToMeasures(IHolyRecord aggregateTableRecord, int cellIndex) {
		List<String> indexToAxis = aggregateTableRecord.getAxes();
		// int[] recordToCubeIndexes = computeInference(indexToAxis, orderedAxes);

		aggregateTableRecord.accept((i, contribution) -> {
			String axis = indexToAxis.get(i);
			if (contribution != null) {
				measureToColumn.entrySet()
						.stream()
						.filter(measure -> ICountMeasuresConstants.COUNT_MEASURED_AXIS.equals(measure.getKey())
								|| measure.getKey().getAxis().equals(axis))
						.map(e -> e.getValue())
						.forEach(column -> {
							column.aggregateObject(cellIndex, contribution);
						});
				// } else
				// LOGGER.warn("Issue accepting contribution {} in {}", contribution, axis);
				// brokenRows.incrementAndGet();
			}
		});

		measureToColumn.entrySet()
				.stream()
				.filter(e -> IOperatorFactory.COUNT.equals(e.getKey().getOperator()))
				.forEach(e -> {
					((IMutableLongAggregatesColumn) e.getValue()).aggregateLong(cellIndex, 1L);
				});
		;
	}

	private int ensureCellRegistration(IntList cellCoordinates) {
		int cellIndex;
		int newCellIndex;
		synchronized (cellToRow) {
			int localCellIndex = cellToRow.getInt(cellCoordinates);
			if (localCellIndex >= 0) {
				cellIndex = localCellIndex;
				newCellIndex = IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
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
				IMutableAxisSmallColumn column = axisToColumn.get(axis);
				int cellAxisCoordinate = cellCoordinates.getInt(axisIndex);
				column.appendCoordinateIndex(cellAxisCoordinate);
			}
		}
		return cellIndex;
	}

	private IntList cellToCoordinates(Map<String, ?> groupBy) {
		registerNewAxes(groupBy.keySet());

		// Append the coordinates for the known axes
		IntList cellCoordinates = indexGroupBy(groupBy);
		return cellCoordinates;
	}

	private void registerNewAxes(Collection<String> inputAxes) {
		// SetView<String> invalidGroupBys = Sets.intersection(axesNames, inputAxes);
		// if (!invalidGroupBys.isEmpty()) {
		// throw new IllegalArgumentException("Can not groupBy aggregated keys: " + invalidGroupBys);
		// }

		if (inputAxes.size() > orderedAxes.size()) {
			// This row introduces a new axes
			Set<String> additionalAxes = new LinkedHashSet<>(inputAxes);
			additionalAxes.removeAll(axisToColumn.keySet());

			orderedAxes.addAll(additionalAxes);
			additionalAxes.forEach(axis -> {
				LOGGER.debug("Registered an additional axis: {}", axis);
				axisToColumn.putIfAbsent(axis, new MutableAxisColumn());
			});
		}
	}

	private IntList cellToCoordinates(IHolyRecord cellsetRecord) {
		List<String> indexToAxis = cellsetRecord.getAxes();

		registerNewAxes(indexToAxis);

		int[] recordToCubeIndexes = computeInference(indexToAxis, orderedAxes);

		IntList cellCoordinates = new IntArrayList(indexToAxis.size());
		cellsetRecord.accept((i, coordinate) -> {
			String axis = orderedAxes.get(recordToCubeIndexes[i]);

			// Register the coordinate
			int coordinateIndex;
			if (coordinate == null || IMutableAxisSmallDictionarySink.NO_COORDINATE == coordinate) {
				coordinateIndex = IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
			} else {
				coordinateIndex = axisToColumn.get(axis).getCoordinateToIndex().getIndexMayAppend(coordinate);
			}

			cellCoordinates.add(coordinateIndex);
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		while (!cellCoordinates.isEmpty() && cellCoordinates
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX) {
			cellCoordinates.removeInt(cellCoordinates.size() - 1);
		}

		return cellCoordinates;
	}

	// This may be cached
	private int[] computeInference(List<String> input, List<String> output) {
		return input.stream().mapToInt(i -> {
			int indexInOutput = output.indexOf(i);
			if (indexInOutput < 0) {
				throw new IllegalStateException(i + " is not in " + output);
			}

			return indexInOutput;
		}).toArray();
	}

	@Override
	public void acceptRowToCell(Stream<? extends IHolyCubeRecord> toAdd) {
		final Meter requests = new Meter();

		toAdd.peek(record -> {
			requests.mark();
			if (requests.getCount() % 100000 == 100000 - 1) {
				LOGGER.debug("We sinked {} rows. Loading at {}rows/sec", requests.getCount(), requests.getMeanRate());
			}
		}).forEach(this::acceptRowToCell);
	}

	protected IntList indexGroupBy(Map<String, ?> groupBy) {
		IntList cellCoordinates = new IntArrayList(groupBy.size());

		// Register the coordinate
		orderedAxes.stream().forEach(axis -> {
			Object coordinate = groupBy.get(axis);

			int coordinateIndex;
			if (coordinate == null || IMutableAxisSmallDictionarySink.NO_COORDINATE == coordinate) {
				coordinateIndex = IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
			} else {
				coordinateIndex = axisToColumn.get(axis).getCoordinateToIndex().getIndexMayAppend(coordinate);
			}

			cellCoordinates.add(coordinateIndex);
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		while (!cellCoordinates.isEmpty() && cellCoordinates
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX) {
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

		List<IAxisCoordinatesDictionary> axisToDictionary =
				Arrays.asList(new IAxisCoordinatesDictionary[orderedAxes.size()]);
		List<IScannableAxisSmallColumn> columns = Arrays.asList(new IScannableAxisSmallColumn[orderedAxes.size()]);

		axisToColumn.forEach((axis, mutableColumn) -> {
			int axisIndex = orderedAxes.indexOf(axis);

			IAxisCoordinatesDictionary immutableDictionary =
					new AxisCoordinatesDictionary(mutableColumn.getCoordinateToIndex().orderedCoordinates());
			axisToDictionary.set(axisIndex, immutableDictionary);

			columns.set(axisIndex, new ImmutableAxisSmallColumn(immutableDictionary, mutableColumn));
		});

		IHolyDictionarizedTable dictionarizedTable = new HolyDictionarizedTable(cellToRow.size(), columns);

		IHasAxesWithCoordinates axisWithCoordinates = new AxisWithCoordinates(orderedAxes, axisToDictionary);
		IHolyCellMultiSet cellSet = new HolyBitmapCellMultiSet(axisWithCoordinates, dictionarizedTable);

		List<IScannableMeasureColumn> aggregatedColumns = aggregations.measures()
				.stream()
				.map(def -> measureToColumn.get(def.asMeasuredAxis()))
				.map(mutableColumn -> {
					if (mutableColumn == null) {
						return null;
					} else {
						return mutableColumn.flush();
					}
				})
				.collect(Collectors.toList());

		IHolyMeasuresTable aggregateTable = new HolyMeasuresTable(aggregations, aggregatedColumns);
		return new HolyCube(nbRows, cellSet, aggregateTable);
	}

}
