package eu.solven.holymolap.mutable.cube;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import eu.solven.holymolap.mutable.axis.SkippedHeaderRows;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableLongAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableLongAggregatesColumn;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
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

	final IHolyMeasuresDefinition measuresDefinition;
	final Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn;
	// final ListMultimap<String, IMutableAggregatesColumn> measuredAxisToColumn;
	final List<String> sharedAggregatedColumns;
	final List<List<IMutableAggregatesColumn>> sharedAggregatedColumnToColumns;
	final List<IMutableLongAggregatesColumn> countColumns;

	// FIFO: we register axes in encounter order, to keep cellToRow consistent. It may not be lexicographical
	final List<String> fifoAxes;
	final List<IMutableAxisSmallColumn> fifoAxisIndexToColumn;

	// A cell is identified by a tuple of coordinateIndexes along axes
	// cellToRow can be recomputed from axisToColumn, but it is necessary to quickly identify to which row an input has
	// to be aggregated into.
	// This can be rebuilt from (fifoAxes, axisToColumn)
	final Object2IntMap<IntList> cellToRow;

	final AtomicLong brokenRows = new AtomicLong();
	final AtomicBoolean closed = new AtomicBoolean();

	final Meter inserts = new Meter();
	final Meter cells = new Meter();

	protected MutableHolyCube(IHolyMeasuresDefinition measuresDefinition,
			Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn,
			List<String> orderedAxis,
			List<IMutableAxisSmallColumn> fifoAxisIndexToColumn,
			Object2IntMap<IntList> cellToRow) {
		this.measuresDefinition = measuresDefinition;
		this.measureToColumn = measureToColumn;

		this.sharedAggregatedColumns = measureToColumn.keySet()
				.stream()
				.map(measuredAxis -> measuredAxis.getAxis())
				.distinct()
				.collect(Collectors.toList());
		this.sharedAggregatedColumnToColumns = sharedAggregatedColumns.stream().map(sharedAggregatedColumn -> {
			return measureToColumn.keySet()
					.stream()
					.filter(ma -> sharedAggregatedColumn.equals(ma.getAxis()))
					.map(measureToColumn::get)
					.collect(Collectors.toList());
		}).collect(Collectors.toList());

		this.countColumns = measureToColumn.entrySet()
				.stream()
				.filter(e -> IOperatorFactory.COUNT.equals(e.getKey().getOperator()))
				.map(e -> (IMutableLongAggregatesColumn) e.getValue())
				.collect(Collectors.toList());

		// this.measuredAxisToColumn = MultimapBuilder.hashKeys().arrayListValues().build();
		// measureToColumn.forEach((measuredAxis, column) -> measuredAxisToColumn.put(measuredAxis.getAxis(), column));

		this.fifoAxes = orderedAxis;
		this.fifoAxisIndexToColumn = fifoAxisIndexToColumn;
		if (orderedAxis.size() != fifoAxisIndexToColumn.size()) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + fifoAxisIndexToColumn.size());
		}
		// else if (!orderedAxis.stream().distinct().collect(Collectors.toSet()).equals(axisToColumn.keySet())) {
		// throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		// }

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
				prepareAxesToColumn(),
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
		return new CopyOnWriteArrayList<>();
	}

	private static List<IMutableAxisSmallColumn> prepareAxesToColumn() {
		// We chose a thread-safe collection not to synchronize any read from it.
		// We expect a low number of writes, hence COpyOnWrite is OK.
		return new CopyOnWriteArrayList<>();
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

	public void acceptRowToCell(IHolyCubeRecord toAdd) {
		inserts.mark();
		if (Long.bitCount(inserts.getCount()) == 1) {
			logSinkRate();
		}

		IntList cellCoordinates = cellToCoordinates(toAdd.getCellsetRecord());

		int cellIndex = ensureCellRegistration(cellCoordinates);

		contributeToMeasures(toAdd.getAggregateTableRecord(), cellIndex);
	}

	private void logSinkRate() {
		LOGGER.info("We sinked {} rows. Loading at {}rows/sec",
				PepperLogHelper.humanBytes(inserts.getCount()),
				inserts.getMeanRate());
	}

	private void contributeToMeasures(IHolyRecord measureTableRecord, int cellIndex) {
		List<String> indexToAxis = measureTableRecord.getAxes();
		int[] recordToCubeIndexes = computeInference(indexToAxis, sharedAggregatedColumns);

		measureTableRecord.accept((i, contribution) -> {
			// String axis = indexToAxis.get(i);
			// if (contribution != null) {
			int aggregatedAxesIndex = recordToCubeIndexes[i];

			if (aggregatedAxesIndex < 0) {
				// Given column does not exist in the cube: it happens if the record is a bit too wide (e.g. because it
				// lazy discard fields)
				return;
			}

			// Multiple measures may rely on the same axis (e.g. input.SUM and input.MAX)
			sharedAggregatedColumnToColumns.get(aggregatedAxesIndex).forEach(column -> {
				column.aggregateObject(cellIndex, contribution);
			});
		});

		countColumns.forEach(column -> column.aggregateLong(cellIndex, 1L));
	}

	private int ensureCellRegistration(IntList cellCoordinates) {
		if (!cellCoordinates.isEmpty() && cellCoordinates
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX) {
			// Else, two equivalent cells would be considered different
			throw new IllegalArgumentException("We do not accept a coordinate ending by NO_COORDINATE_REF");
		}

		int cellIndex;
		int newCellIndex;
		synchronized (cellToRow) {
			int localCellIndex = cellToRow.getInt(cellCoordinates);
			if (localCellIndex >= 0) {
				cellIndex = localCellIndex;
				newCellIndex = IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
			} else {
				newCellIndex = getNbRows();
				cellIndex = newCellIndex;
				cellToRow.put(cellCoordinates, newCellIndex);
			}
		}

		if (newCellIndex >= 0) {
			cells.mark();
			if (Long.bitCount(inserts.getCount()) == 1) {
				logCellRate();
			}

			// Register this new cell
			for (int axisIndex = 0; axisIndex < cellCoordinates.size(); axisIndex++) {
				IMutableAxisSmallColumn column = fifoAxisIndexToColumn.get(axisIndex);
				int cellAxisCoordinate = cellCoordinates.getInt(axisIndex);
				column.appendCoordinateRef(cellAxisCoordinate);
			}
		}
		return cellIndex;
	}

	private void logCellRate() {
		LOGGER.info("We registered {} cells. Loading at {}cells/sec",
				PepperLogHelper.humanBytes(cells.getCount()),
				cells.getMeanRate());
	}

	private void registerNewAxes(Collection<String> inputAxes) {
		// This row introduces a new axes
		Set<String> additionalAxes = new LinkedHashSet<>(inputAxes);
		additionalAxes.removeAll(fifoAxes);

		fifoAxes.addAll(additionalAxes);
		additionalAxes.forEach(axis -> {
			LOGGER.debug("Registered an additional axis: {}", axis);
			int nbRows = getNbRows();
			MutableAxisColumn rawNewColumn = new MutableAxisColumn();
			IMutableAxisSmallColumn newColumn;

			if (nbRows == 0) {
				newColumn = rawNewColumn;
			} else {
				newColumn = new SkippedHeaderRows(nbRows, rawNewColumn);
			}

			fifoAxisIndexToColumn.add(newColumn);
		});
		// }
	}

	private IntList cellToCoordinates(IHolyRecord cellsetRecord) {
		List<String> indexToAxis = cellsetRecord.getAxes();

		if (indexToAxis.isEmpty()) {
			return IntLists.emptyList();
		}

		registerNewAxes(indexToAxis);

		int[] recordToCubeIndexes = computeInference(indexToAxis, fifoAxes);
		// This record will no write further than this
		int maxIndex = IntStream.of(recordToCubeIndexes).max().getAsInt();

		int[] noRef = new int[maxIndex + 1];
		Arrays.fill(noRef, IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX);

		IntList cellCoordinates = new IntArrayList(noRef);

		cellsetRecord.accept((i, coordinate) -> {
			int cubeIndex = recordToCubeIndexes[i];

			// Register the coordinate
			int coordinateRef;
			if (coordinate == null) {
				coordinateRef = IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
			} else {
				coordinateRef = fifoAxisIndexToColumn.get(cubeIndex).getCoordinateToRef().getIndexMayAppend(coordinate);
			}

			int previousRef = cellCoordinates.set(cubeIndex, coordinateRef);

			// The following fails typically if IHolyRecord.accept process a single index multiple times
			assert previousRef == IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX;
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		// It handles 2 equivalent cell, when known axes having trailing (not contributed axes)
		while (!cellCoordinates.isEmpty() && cellCoordinates
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionarySink.NO_COORDINATE_INDEX) {
			cellCoordinates.removeInt(cellCoordinates.size() - 1);
		}

		return cellCoordinates;
	}

	// This may be cached
	/**
	 * 
	 * @param input
	 * @param output
	 * @return the index of input within output
	 */
	protected int[] computeInference(List<String> input, List<String> output) {
		return input.stream().mapToInt(i -> {
			int indexInOutput = output.indexOf(i);
			if (indexInOutput < 0) {
				// We will simply do not consume this field
				// throw new IllegalStateException(i + " is not in " + output);
				return -1;
			}

			return indexInOutput;
		}).toArray();
	}

	@Override
	public void acceptRowToCell(Stream<? extends IHolyCubeRecord> toAdd) {
		toAdd.forEach(this::acceptRowToCell);
	}

	@Override
	public IHolyCube closeToHolyCube() {
		if (!closed.compareAndSet(false, true)) {
			throw new IllegalStateException("Was already closed");
		}

		logSinkRate();
		logCellRate();

		int nbAxes = fifoAxes.size();

		List<IAxisCoordinatesDictionary> axisToDictionary = new ArrayList<>(nbAxes);
		List<IScannableAxisSmallColumn> columns = new ArrayList<>(nbAxes);

		List<String> orderedAxesAsList = fifoAxes.stream().sorted().collect(Collectors.toList());

		orderedAxesAsList.forEach(axis -> {
			int fifoAxisIndex = fifoAxes.indexOf(axis);
			IMutableAxisSmallColumn mutableColumn = fifoAxisIndexToColumn.get(fifoAxisIndex);
			IAxisCoordinatesDictionary immutableDictionary =
					new AxisCoordinatesDictionary(mutableColumn.getCoordinateToRef().orderedCoordinates());

			axisToDictionary.add(immutableDictionary);
			columns.add(new ImmutableAxisSmallColumn(immutableDictionary, mutableColumn));
		});

		int nbRows = getNbRows();
		IHolyDictionarizedTable dictionarizedTable = new HolyDictionarizedTable(nbRows, columns);

		IHasAxesWithCoordinates axisWithCoordinates = new AxisWithCoordinates(orderedAxesAsList, axisToDictionary);
		IHolyCellMultiSet cellSet = new HolyBitmapCellMultiSet(axisWithCoordinates, dictionarizedTable);

		List<IScannableMeasureColumn> aggregatedColumns = measuresDefinition.measures()
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

		IHolyMeasuresTable aggregateTable = new HolyMeasuresTable(measuresDefinition, aggregatedColumns);

		return new HolyCube(nbRows, cellSet, aggregateTable);
	}

	private int getNbRows() {
		return cellToRow.size();
	}

}
