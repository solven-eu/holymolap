package eu.solven.holymolap.mutable.cube;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.primitives.Ints;

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
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallColumn;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionary;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallIntDictionary;
import eu.solven.holymolap.mutable.axis.IProxyForMutableAxisSmallDictionary;
import eu.solven.holymolap.mutable.axis.MutableAxisColumn;
import eu.solven.holymolap.mutable.axis.SkippedHeaderRows;
import eu.solven.holymolap.mutable.cellset.IHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.FibonacciHolyCellToRow;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableLongAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.MutableLongAggregatesColumn;
import eu.solven.holymolap.primitives.IntArrayListFastHashCode;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

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
	final IHolyCellToRow cellToRow;

	final AtomicLong brokenRows = new AtomicLong();
	final AtomicBoolean closed = new AtomicBoolean();

	final Meter inserts = new Meter();
	final Meter cells = new Meter();
	final Meter measures = new Meter();

	protected MutableHolyCube(IHolyMeasuresDefinition measuresDefinition,
			Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn,
			List<String> orderedAxis,
			List<IMutableAxisSmallColumn> fifoAxisIndexToColumn,
			IHolyCellToRow cellToRow) {
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
				.filter(e -> IStandardOperators.COUNT.equals(e.getKey().getOperator()))
				.map(e -> (IMutableLongAggregatesColumn) e.getValue())
				.collect(Collectors.toList());

		this.fifoAxes = orderedAxis;
		this.fifoAxisIndexToColumn = fifoAxisIndexToColumn;
		if (orderedAxis.size() != fifoAxisIndexToColumn.size()) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + fifoAxisIndexToColumn.size());
		}
		// else if (!orderedAxis.stream().distinct().collect(Collectors.toSet()).equals(axisToColumn.keySet())) {
		// throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		// }

		this.cellToRow = cellToRow;
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

	private static IHolyCellToRow prepareCellToRow() {
		// return new Object2IntHolyCellToRow();
		return new FibonacciHolyCellToRow();
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
		IntList cellCoordinates = cellToCoordinates(toAdd.getCellsetRecord());

		int cellIndex = ensureCellRegistration(cellCoordinates);

		contributeToMeasures(toAdd.getAggregateTableRecord(), cellIndex);

		inserts.mark();
		if (Long.bitCount(inserts.getCount()) == 1) {
			logSinkRate();
		}
	}

	@Override
	public void acceptRowToCell(IHolyRecordsTable cellsToAdd, IHolyRecordsTable measuresToAdd) {
		long size = cellsToAdd.size();
		if (size != measuresToAdd.size()) {
			throw new IllegalArgumentException("Inconsistency between " + cellsToAdd + " and " + measuresToAdd);
		} else if (size > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("TODO");
		}

		NavigableMap<Integer, int[]> axisIndexToCoordinates = cellToCoordinates(cellsToAdd, size);

		// .ensureCellRegistration
		int[] cellIndexes = ensureCellRegistration(size, axisIndexToCoordinates);

		contributeToMeasures(measuresToAdd, size, cellIndexes);

		long markBefore = inserts.getCount();
		inserts.mark(size);
		long markAfter = inserts.getCount();
		if (nextPowerOf2(markBefore) != nextPowerOf2(markAfter)) {
			logSinkRate();
		}
	}

	private NavigableMap<Integer, int[]> cellToCoordinates(IHolyRecordsTable cellsToAdd, long size) {
		NavigableMap<Integer, int[]> axisIndexToCoordinates;
		List<String> indexToAxis = cellsToAdd.getAxes();
		registerNewAxes(indexToAxis);
		int[] recordToCubeIndexes = computeInference(indexToAxis, fifoAxes);

		// This record will no write further than this
		// int maxIndex = IntStream.of(recordToCubeIndexes).max().getAsInt();

		axisIndexToCoordinates = new ConcurrentSkipListMap<>();

		int sizeAsInt = Ints.checkedCast(size);

		cellsToAdd.accept((axisIndex, list) -> {
			int cubeAxisIndex = recordToCubeIndexes[axisIndex];

			int[] fieldCoordinatesRef = new int[sizeAsInt];
			Arrays.fill(fieldCoordinatesRef, IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

			axisIndexToCoordinates.put(cubeAxisIndex, fieldCoordinatesRef);

			IProxyForMutableAxisSmallDictionary proxyForCoordinateToRef =
					fifoAxisIndexToColumn.get(cubeAxisIndex).getCoordinateToRef();
			if (list instanceof IntList) {
				IntList intList = (IntList) list;
				IMutableAxisSmallIntDictionary coordinateToRef = proxyForCoordinateToRef.asInts();

				for (int i = 0; i < size; i++) {
					int coordinate = intList.getInt(i);

					// Register the coordinate
					int coordinateRef = coordinateToRef.getIntIndexMayAppend(coordinate);

					int previousRef = fieldCoordinatesRef[i];
					fieldCoordinatesRef[i] = coordinateRef;

					// The following fails typically if IHolyRecord.accept process a single index multiple times
					assert previousRef == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
				}

				String axis = fifoAxes.get(cubeAxisIndex);
				LOGGER.info("axis={} has now cardinality={}",
						axis,
						PepperLogHelper.humanBytes(coordinateToRef.cardinality()));
			} else {
				IMutableAxisSmallDictionary coordinateToRef = proxyForCoordinateToRef.asObjects();

				// list may be bigger if buffered (elements out of size may be anything)
				// list may be smaller is last element would be nulls
				int realSize = Math.min(Ints.checkedCast(size), list.size());
				for (int i = 0; i < realSize; i++) {
					Object coordinate = list.get(i);

					// Register the coordinate
					int coordinateRef;
					if (coordinate == null) {
						coordinateRef = IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
					} else {
						coordinateRef = coordinateToRef.getIndexMayAppend(coordinate);
					}

					int previousRef = fieldCoordinatesRef[i];
					fieldCoordinatesRef[i] = coordinateRef;

					// The following fails typically if IHolyRecord.accept process a single index multiple times
					assert previousRef == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
				}

				String axis = fifoAxes.get(cubeAxisIndex);
				LOGGER.info("axis={} has now cardinality={}",
						axis,
						PepperLogHelper.humanBytes(coordinateToRef.cardinality()));
			}
		});
		return axisIndexToCoordinates;
	}

	/**
	 * 
	 * @param size
	 *            the number of rows being inserted
	 * @param axisIndexToCoordinates
	 *            a mapping from axisIndex to the coordinates (rowIndex -> coordinateRef)
	 * @return
	 */
	private int[] ensureCellRegistration(long size, NavigableMap<Integer, int[]> axisIndexToCoordinates) {
		IntArrayList cellIndexesAsList = new IntArrayList();

		// This record will no write further than this
		int maxIndex = axisIndexToCoordinates.lastKey().intValue();

		int[] noRef = new int[maxIndex + 1];
		Arrays.fill(noRef, IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		IntList tmpCellCoordinates = new IntArrayList(noRef);
		for (int rowIndex = 0; rowIndex < size; rowIndex++) {
			final int fRowIndex = rowIndex;
			tmpCellCoordinates.setElements(noRef);

			axisIndexToCoordinates.forEach(
					(axisIndex, coordinates) -> tmpCellCoordinates.set(axisIndex.intValue(), coordinates[fRowIndex]));

			IntList rowCellCoordinates = tmpCellCoordinates;

			// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
			// It handles 2 equivalent cell, when known axes having trailing (not contributed axes)
			if (!rowCellCoordinates.isEmpty() && rowCellCoordinates
					.getInt(rowCellCoordinates.size() - 1) == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
				// There is trailing -1
				int newSize = rowCellCoordinates.size() - 1;

				while (!rowCellCoordinates.isEmpty()
						&& rowCellCoordinates.getInt(newSize - 1) == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
					newSize--;
				}

				rowCellCoordinates = rowCellCoordinates.subList(0, newSize);
			}

			int cellIndex;
			int newCellIndex;
			synchronized (cellToRow) {
				int localCellIndex = cellToRow.getRow(rowCellCoordinates);
				if (localCellIndex >= 0) {
					cellIndex = localCellIndex;
					newCellIndex = IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
				} else {
					// Copy as we relied on a buffer
					// But this is not necessary for FibonacciHolyCellToRow
					newCellIndex = cellToRow.registerRow(new IntArrayListFastHashCode(rowCellCoordinates));
					cellIndex = newCellIndex;
				}
			}

			if (newCellIndex >= 0) {
				cells.mark();
				if (Long.bitCount(cells.getCount()) == 1) {
					logCellRate();
				}

				// Register this new cell
				for (int axisIndex = 0; axisIndex < rowCellCoordinates.size(); axisIndex++) {
					IMutableAxisSmallColumn column = fifoAxisIndexToColumn.get(axisIndex);
					int cellAxisCoordinate = rowCellCoordinates.getInt(axisIndex);
					column.appendCoordinateRef(cellAxisCoordinate);
				}
			}
			cellIndexesAsList.add(cellIndex);
		}

		return cellIndexesAsList.elements();
	}

	private void contributeToMeasures(IHolyRecordsTable measuresToAdd, long size, int[] cellIndexes) {
		List<String> indexToAxis = measuresToAdd.getAxes();
		int[] recordToCubeIndexes = computeInference(indexToAxis, sharedAggregatedColumns);

		measuresToAdd.accept((recordAggregatedIndex, list) -> {
			int cubeAggregatedIndex = recordToCubeIndexes[recordAggregatedIndex];

			if (cubeAggregatedIndex < 0) {
				// Given column does not exist in the cube: it happens if the record is a bit too wide (e.g. because
				// it lazy discard fields)
				return;
			}

			List<IMutableAggregatesColumn> columns = sharedAggregatedColumnToColumns.get(cubeAggregatedIndex);
			for (int rowIndex = 0; rowIndex < size; rowIndex++) {
				int cellIndex = cellIndexes[rowIndex];

				if (list instanceof DoubleList) {
					double contribution = ((DoubleList) list).getDouble(rowIndex);

					// Multiple measures may rely on the same axis (e.g. input.SUM and input.MAX)
					columns.forEach(column -> {
						if (column instanceof IMutableDoubleAggregatesColumn) {
							((IMutableDoubleAggregatesColumn) column).aggregateDouble(cellIndex, contribution);
						} else {
							column.aggregateObject(cellIndex, contribution);
						}
					});
				} else {
					Object contribution = list.get(rowIndex);

					// Multiple measures may rely on the same axis (e.g. input.SUM and input.MAX)
					columns.forEach(column -> {
						column.aggregateObject(cellIndex, contribution);
					});
				}

				measures.mark();
				if (Long.bitCount(measures.getCount()) == 1) {
					logMeasureRate();
				}
			}
		});

		// We may have only count measures
		if (!countColumns.isEmpty()) {
			for (int rowIndex = 0; rowIndex < size; rowIndex++) {
				int cellIndex = cellIndexes[rowIndex];
				countColumns.forEach(column -> column.aggregateLong(cellIndex, 1L));
			}
		}
	}

	private void logSinkRate() {
		// TODO Log only since a new row this previous log
		LOGGER.info("We sinked {} rows. Loading at {}rows/sec",
				PepperLogHelper.humanBytes(inserts.getCount()),
				inserts.getMeanRate());
	}

	// https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	private long nextPowerOf2(long l) {
		l--;
		l |= l >> 1;
		l |= l >> 2;
		l |= l >> 4;
		l |= l >> 8;
		l |= l >> 16;
		l |= l >> 32;
		l |= l >> 64;
		l++;

		return l;
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
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			// Else, two equivalent cells would be considered different
			throw new IllegalArgumentException("We do not accept a coordinate ending by NO_COORDINATE_REF");
		}

		int cellIndex;
		int newCellIndex;
		synchronized (cellToRow) {
			int localCellIndex = cellToRow.getRow(cellCoordinates);
			if (localCellIndex >= 0) {
				cellIndex = localCellIndex;
				newCellIndex = IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
			} else {
				newCellIndex = cellToRow.registerRow(cellCoordinates);
				cellIndex = newCellIndex;
			}
		}

		if (newCellIndex >= 0) {
			cells.mark();
			if (Long.bitCount(cells.getCount()) == 1) {
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

	private void logMeasureRate() {
		LOGGER.info("We contributed {} measureAggregates. Loading at {} measureAggregates/sec",
				PepperLogHelper.humanBytes(measures.getCount()),
				measures.getMeanRate());
	}

	private void registerNewAxes(Collection<String> inputAxes) {
		// This row introduces a new axes
		Set<String> additionalAxes = new LinkedHashSet<>(inputAxes);
		additionalAxes.removeAll(fifoAxes);

		fifoAxes.addAll(additionalAxes);
		additionalAxes.forEach(axis -> {
			LOGGER.debug("Registered an additional axis: {}", axis);
			int nbRows = getNbCells();
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
		Arrays.fill(noRef, IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		IntList cellCoordinates = new IntArrayList(noRef);

		cellsetRecord.accept((i, coordinate) -> {
			int cubeIndex = recordToCubeIndexes[i];

			// Register the coordinate
			int coordinateRef;
			if (coordinate == null) {
				coordinateRef = IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
			} else {
				coordinateRef = fifoAxisIndexToColumn.get(cubeIndex).getCoordinateToRef().getIndexMayAppend(coordinate);
			}

			int previousRef = cellCoordinates.set(cubeIndex, coordinateRef);

			// The following fails typically if IHolyRecord.accept process a single index multiple times
			assert previousRef == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		// It handles 2 equivalent cell, when known axes having trailing (not contributed axes)
		while (!cellCoordinates.isEmpty() && cellCoordinates
				.getInt(cellCoordinates.size() - 1) == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			cellCoordinates.removeInt(cellCoordinates.size() - 1);
		}

		return cellCoordinates;
	}

	// This may be cached
	/**
	 * 
	 * @param inputAxes
	 * @param outputAxes
	 * @return the index of input within output
	 */
	protected int[] computeInference(List<String> inputAxes, List<String> outputAxes) {
		return inputAxes.stream().mapToInt(inputAxis -> {
			int indexInOutput = outputAxes.indexOf(inputAxis);
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

		int nbCells = getNbCells();
		IHolyDictionarizedTable dictionarizedTable = new HolyDictionarizedTable(nbCells, columns);

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

		return new HolyCube(nbCells, cellSet, aggregateTable);
	}

	private int getNbCells() {
		return cellToRow.size();
	}

}
