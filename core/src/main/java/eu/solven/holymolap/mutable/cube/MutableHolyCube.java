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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.HolyBitmapCellMultiSet;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.factory.HolyDataStructuresFactory;
import eu.solven.holymolap.factory.IHolyDataStructuresFactory;
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
import eu.solven.holymolap.mutable.cellset.IAppendableHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.IReadableHolyCellToRow;
import eu.solven.holymolap.mutable.column.IMutableAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableDoubleAggregatesColumn;
import eu.solven.holymolap.mutable.column.IMutableLongAggregatesColumn;
import eu.solven.holymolap.primitives.IntArrayListFastHashCode;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.record.HolyMeasuresSingleRecordTable;
import eu.solven.holymolap.sink.record.HolySingleRecordTable;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyMeasuresRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Enables loading data into an {@link IHolyCube}. It is specialized for loading. It may be queried, but with a chance
 * of inconsistencies as some view may return holding partial rows.
 * 
 * @author Benoit Lacelle
 *
 */
public class MutableHolyCube implements IMutableHolyCube {
	private static final Logger LOGGER = LoggerFactory.getLogger(MutableHolyCube.class);

	final LoadingContext loadingContext;
	final IHolyDataStructuresFactory factory;

	final IHolyMeasuresDefinition measuresDefinition;
	final Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn;
	// final ListMultimap<String, IMutableAggregatesColumn> measuredAxisToColumn;
	final List<IMeasuredAxis> orderedMeasuredAxis;
	final List<IMutableAggregatesColumn> measuredIndexToColumn;
	// Count columns are separated as they are not contributed from input records content.
	final List<IMutableLongAggregatesColumn> countColumns;

	// FIFO: we register axes in encounter order, to keep cellToRow consistent. It may not be lexicographical
	final List<String> fifoAxes;
	final List<IMutableAxisSmallColumn> fifoAxisIndexToColumn;

	// A cell is identified by a tuple of coordinateIndexes along axes
	// cellToRow can be recomputed from axisToColumn, but it is necessary to quickly identify to which row an input has
	// to be aggregated into.
	// This can be rebuilt from (fifoAxes, axisToColumn)
	final IAppendableHolyCellToRow cellToRow;

	final AtomicLong brokenRows = new AtomicLong();
	final AtomicBoolean closed = new AtomicBoolean();

	protected MutableHolyCube(IHolyDataStructuresFactory factory,
			LoadingContext loadingContext,
			IHolyMeasuresDefinition measuresDefinition,
			Map<IMeasuredAxis, IMutableAggregatesColumn> measureToColumn,
			List<String> orderedAxis,
			List<IMutableAxisSmallColumn> fifoAxisIndexToColumn,
			IAppendableHolyCellToRow cellToRow) {
		this.factory = factory;
		this.loadingContext = loadingContext;

		this.measuresDefinition = measuresDefinition;
		this.measureToColumn = measureToColumn;

		this.orderedMeasuredAxis = measureToColumn.keySet().stream().distinct().collect(Collectors.toList());
		this.measuredIndexToColumn = orderedMeasuredAxis.stream()
				.map(measuredAxis -> measureToColumn.get(measuredAxis))
				.collect(Collectors.toList());

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
	 * @param loadingContext
	 */
	public MutableHolyCube(IHolyDataStructuresFactory factory,
			LoadingContext loadingContext,
			IHolyMeasuresDefinition aggregations) {
		this(factory,
				loadingContext,
				aggregations,
				prepareAggregationColumns(factory, aggregations),
				prepareOrderedAxes(),
				prepareAxesToColumn(),
				factory.makeCellToRow());
	}

	public MutableHolyCube(IHolyMeasuresDefinition aggregations) {
		this(new HolyDataStructuresFactory(), new LoadingContext(), aggregations);
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
			IHolyDataStructuresFactory factory,
			IHolyMeasuresDefinition aggregations) {
		OperatorFactory operatorFactory = factory.makeOperatorFactory();

		return aggregations.measures()
				.stream()
				.collect(Collectors.toMap(a -> a.asMeasuredAxis(),
						a -> provisionAggregateColumn(factory, operatorFactory, a.asMeasuredAxis())));
	}

	protected static IMutableAggregatesColumn provisionAggregateColumn(IHolyDataStructuresFactory factory,
			IOperatorFactory operatorFactory,
			IMeasuredAxis measure) {
		IBinaryOperator binaryOperator = operatorFactory.getBinaryOperator(measure.getOperator());

		if (binaryOperator instanceof IDoubleBinaryOperator) {
			return factory.makeMutableDoubleAggregatesColumn((IDoubleBinaryOperator) binaryOperator);
		} else if (binaryOperator instanceof ILongBinaryOperator) {
			return factory.makeMutableLongAggregatesColumn((ILongBinaryOperator) binaryOperator);
		} else {
			return factory.makeMutableAggregatesColumn(binaryOperator);
		}
	}

	@Override
	public void acceptRowToCell(IHolyRecordsTable cellsToAdd, IHolyMeasuresRecordsTable measuresToAdd) {
		long size = cellsToAdd.size();
		if (size != measuresToAdd.size()) {
			throw new IllegalArgumentException("Inconsistency between " + cellsToAdd + " and " + measuresToAdd);
		} else if (size > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("TODO");
		}

		NavigableMap<Integer, int[]> axisIndexToCoordinates = cellToCoordinates(cellsToAdd, size);

		int[] cellIndexes = ensureCellRegistration(size, axisIndexToCoordinates);

		contributeToMeasures(measuresToAdd, size, cellIndexes);

		loadingContext.markInsert(size);
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

		int[] noRef;
		if (axisIndexToCoordinates.isEmpty()) {
			noRef = new int[0];
		} else {
			// This record will no write further than this
			int maxIndex = axisIndexToCoordinates.lastKey().intValue();

			noRef = new int[maxIndex + 1];
			Arrays.fill(noRef, IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);
		}

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

			int positiveOrNegativeCellIndex =
					cellToRow.getMayAppendRow(new IntArrayListFastHashCode(rowCellCoordinates));

			int cellIndex;
			int newCellIndex;
			if (positiveOrNegativeCellIndex >= 0) {
				cellIndex = positiveOrNegativeCellIndex;
				newCellIndex = IMutableAxisSmallDictionary.NO_COORDINATE_INDEX;
			} else {
				cellIndex = -positiveOrNegativeCellIndex - 1;
				newCellIndex = cellIndex;
			}

			if (newCellIndex >= 0) {
				loadingContext.markNewCell(1);

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

	private void contributeToMeasures(IHolyMeasuresRecordsTable measuresRecordTable, long size, int[] cellIndexes) {
		List<IMeasuredAxis> indexToAxis = measuresRecordTable.getMeasures();
		int[] recordToCubeIndexes = computeInference(indexToAxis, orderedMeasuredAxis);

		measuresRecordTable.accept((recordMeasureIndex, list) -> {
			int cubeMeasureIndex = recordToCubeIndexes[recordMeasureIndex];

			if (cubeMeasureIndex < 0) {
				// Given column does not exist in the cube: it happens if the record is a bit too wide (e.g. because
				// it lazy discard fields)
				return;
			}

			IMutableAggregatesColumn column = measuredIndexToColumn.get(cubeMeasureIndex);
			for (int rowIndex = 0; rowIndex < size; rowIndex++) {
				int cellIndex = cellIndexes[rowIndex];

				if (list instanceof DoubleList) {
					double contribution = ((DoubleList) list).getDouble(rowIndex);

					// Multiple measures may rely on the same axis (e.g. input.SUM and input.MAX)
					if (column instanceof IMutableDoubleAggregatesColumn) {
						((IMutableDoubleAggregatesColumn) column).aggregateDouble(cellIndex, contribution);
					} else {
						column.aggregateObject(cellIndex, contribution);
					}
				} else {
					Object contribution = list.get(rowIndex);

					// Multiple measures may rely on the same axis (e.g. input.SUM and input.MAX)
					column.aggregateObject(cellIndex, contribution);
				}

				loadingContext.markMeasureContributions(1);
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

	// This may be cached
	/**
	 * 
	 * @param inputAxes
	 * @param outputAxes
	 * @return the index of input within output
	 */
	protected <T> int[] computeInference(List<T> inputAxes, List<T> outputAxes) {
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
		toAdd.forEach(r -> acceptRowToCell(new HolySingleRecordTable(r.getCellsetRecord()),
				new HolyMeasuresSingleRecordTable(r.getMeasuresTableRecord())));
	}

	@Override
	public IHolyCube closeToHolyCube() {
		if (!closed.compareAndSet(false, true)) {
			throw new IllegalStateException("Was already closed");
		}

		loadingContext.logSinkRate();
		loadingContext.logCellRate();

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
						return mutableColumn.flush(factory);
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
