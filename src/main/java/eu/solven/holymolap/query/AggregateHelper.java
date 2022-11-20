package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.aggregate.NiceCellToAggregate;
import eu.solven.holymolap.aggregate.RawCellToAggregate;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.immutable.axes.IHasNavigableAxes;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.measures.aggregation.IAggregationLogic;
import eu.solven.holymolap.measures.aggregation.LongAggregationLogic;
import eu.solven.holymolap.measures.operator.IStandardLongOperators;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.utils.HolyIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class AggregateHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHelper.class);

	private static int[] computeAxesIndexes(IHasNavigableAxes navigableAxes, final List<String> queriedAxes) {
		// This array will hold the indexes of the values of the next coordinate
		// matching the selected wildcards
		int[] axesIndexes = new int[queriedAxes.size()];
		{
			int i = -1;
			for (String wildcardKey : queriedAxes) {
				i++;

				int wildcardKeyIndex = navigableAxes.getAxisIndex(wildcardKey);

				axesIndexes[i] = wildcardKeyIndex;
			}

		}
		return axesIndexes;
	}

	public static Stream<RawCoordinatesToBitmap> queryToRawCellsIterator(final IHolyCube cube,
			final IAggregationQuery query) {
		return queryToRawCellsIterator(cube, query, cube.getCellSet().getTable().getAll());
	}

	protected static Stream<RawCoordinatesToBitmap> queryToRawCellsIterator(final IHolyCube cube,
			final IAggregationQuery query,
			RoaringBitmap withinMe) {
		IHolyCellMultiSet cellSet = cube.getCellSet();

		IHasNavigableAxes navigableAxes = cellSet.getAxesWithCoordinates();

		List<String> wildcardAxes = query.getAxes();
		int[] axesIndexes = computeAxesIndexes(navigableAxes, wildcardAxes);

		int someUnknownAxis = Ints.indexOf(axesIndexes, -1);
		if (someUnknownAxis >= 0) {
			// We are decomposing an unknown axis
			LOGGER.debug("{} is unknown amongst {}", wildcardAxes.get(someUnknownAxis), navigableAxes.getAxes());
			return Stream.empty();
		}

		return new AggregationHelper2()
				.asStream(cellSet, axesIndexes, RoaringBitmap.and(cube.getFiltersBitmap(query), withinMe));
	}

	public static Stream<RawCellToAggregate<List<?>>> queryToRawCellsIterator(final ICompositeHolyCube compositeCube,
			final IAggregationQuery query) {
		List<IHolyCube> partitions = ImmutableList.copyOf(compositeCube.partitions());

		// We pre-compute the filter condition for each cube
		List<RoaringBitmap> partitionToLeftovers = new ArrayList<>();
		IntStream.range(0, partitions.size())
				.forEach(i -> partitionToLeftovers.add(partitions.get(i).getFiltersBitmap(query)));

		// Will hold null for unknown measures
		List<IAggregationLogic<?>> compositeAggregationLogics =
				getAggregationLogics(compositeCube.getMeasuresDefinition(), query);

		// We will process cube one-by-one, considering all slices for given cube, then moving to next cube for
		// not-covered slices
		return IntStream.range(0, partitions.size()).mapToObj(currentCubeIndex -> {
			IHolyCube head = partitions.get(currentCubeIndex);

			RoaringBitmap leftIfHead = partitionToLeftovers.get(currentCubeIndex);
			Stream<RawCoordinatesToBitmap> headCells = queryToRawCellsIterator(head, query, leftIfHead);

			final int currentCubeIndexFinal = currentCubeIndex;

			// Iterate through slices of the head
			return headCells.flatMap(headCoordinate -> {
				CoordinatesRefs sliceCoordinates = headCoordinate.getSlice();

				IHolyMeasuresTable headMeasuresTable = head.getMeasuresTable();

				RawCellToAggregate<List<?>> headAggregates =
						cellsToAggregates(headMeasuresTable, compositeAggregationLogics).apply(headCoordinate);

				List<RawCellToAggregate<List<?>>> aggregatesPartitions = new ArrayList<>();
				aggregatesPartitions.add(headAggregates);

				List<IBinaryOperator> headOperators = compositeAggregationLogics.stream()
						.map(al -> al == null ? null : al.getOperator())
						.collect(Collectors.toList());

				// Scan the same slice for other cubes
				for (int nextCubeIndex = currentCubeIndexFinal + 1; nextCubeIndex < partitions
						.size(); nextCubeIndex++) {
					IHolyCube next = partitions.get(nextCubeIndex);

					IHasAxesWithCoordinates nextAxesWithCoordinates = next.getCellSet().getAxesWithCoordinates();
					long[] coordinatesForNext = transcodeCoordinatesRef(sliceCoordinates, nextAxesWithCoordinates);

					if (Longs.indexOf(coordinatesForNext, IHasAxesWithCoordinates.NOT_INDEXED) >= 0) {
						// This slice is not materialized in the next cube
						LOGGER.debug("This slice does not exist in the next cube");
					} else {
						int[] nextAxesIndexes =
								computeAxesIndexes(head.getCellSet().getAxesWithCoordinates(), query.getAxes());

						// Compute matching rows in next
						RoaringBitmap nextMatchingSlice =
								next.getCellSet().getTable().getCoordinateToRows(nextAxesIndexes, coordinatesForNext);

						RoaringBitmap nextLeftovers = partitionToLeftovers.get(nextCubeIndex);

						// Register these rows as processed
						partitionToLeftovers.set(nextCubeIndex, RoaringBitmap.andNot(nextLeftovers, nextMatchingSlice));

						RoaringBitmap nextToConsider = RoaringBitmap.and(nextLeftovers, nextMatchingSlice);
						// Compute aggregates in next
						IHolyMeasuresTable nextMeasuresTable = next.getMeasuresTable();
						List<IAggregationLogic<?>> nextAggregationLogics =
								getAggregationLogics(nextMeasuresTable.getMeasuresDefinition(), query);
						RawCellToAggregate<List<?>> nextAggregates =
								cellsToAggregates(nextMeasuresTable, nextAggregationLogics)
										.apply(new RawCoordinatesToBitmap(sliceCoordinates, nextToConsider));
						aggregatesPartitions.add(nextAggregates);
					}
				}

				if (aggregatesPartitions.isEmpty()) {
					return Stream.empty();
				}

				RawCellToAggregate<List<?>> mergedAggregates = mergeAggregates(headOperators, aggregatesPartitions);
				return Stream.of(mergedAggregates);
			});
		}).flatMap(s -> s);

	}

	private static RawCellToAggregate<List<?>> mergeAggregates(List<IBinaryOperator> headOperators,
			List<RawCellToAggregate<List<?>>> aggregatesPartitions) {
		if (aggregatesPartitions.isEmpty()) {
			throw new IllegalStateException("Should have be caught earlier");
		}

		RawCellToAggregate<List<?>> firstPartition = aggregatesPartitions.get(0);
		if (aggregatesPartitions.size() == 1) {
			return firstPartition;
		}

		List<Object> aggregates = new ArrayList<>(firstPartition.getMeasureValue());
		for (int i = 1; i < aggregatesPartitions.size(); i++) {
			for (int measureIndex = 0; measureIndex < headOperators.size(); measureIndex++) {
				IBinaryOperator operator = headOperators.get(measureIndex);
				Object currentAggregation = aggregates.get(measureIndex);
				Object partitionAggregate = aggregatesPartitions.get(i).getMeasureValue().get(measureIndex);
				Object newAggregate = operator.apply(currentAggregation, partitionAggregate);
				aggregates.set(measureIndex, newAggregate);
			}
		}

		return new RawCellToAggregate<List<?>>(firstPartition.getCoordinatesRefs(), aggregates);
	}

	private static long[] transcodeCoordinatesRef(CoordinatesRefs input,
			IHasAxesWithCoordinates targetAxesWithCoordinates) {
		int length = input.getCoordinatesRef().length;
		int[] targetAxes = new int[length];
		long[] targetCoordinates = new long[length];

		IHasAxesWithCoordinates axesWithCoordinates = input.getAxesWithCoordinates();

		for (int i = 0; i < targetCoordinates.length; i++) {
			int inputAxis = input.getAxesIndexes()[i];

			String axisName = axesWithCoordinates.indexToAxis(inputAxis);
			int targetAxis = targetAxesWithCoordinates.getAxisIndex(axisName);

			long inputRef = input.getCoordinatesRef()[i];
			Object coordinate = axesWithCoordinates.dereferenceCoordinate(inputAxis, inputRef);

			targetAxes[i] = targetAxis;
			targetCoordinates[i] = targetAxesWithCoordinates.getCoordinateRef(targetAxis, coordinate);
		}

		return targetCoordinates;
	}

	public static Stream<NiceCellToAggregate<List<?>>> queryToNiceCellsIterator(final IHolyCube cube,
			final IAggregationQuery query) {
		IHolyMeasuresTable measuresTable = cube.getMeasuresTable();
		List<IAggregationLogic<?>> aggregationLogics =
				getAggregationLogics(measuresTable.getMeasuresDefinition(), query);

		if (aggregationLogics.isEmpty()) {
			return Stream.empty();
		}

		return queryToRawCellsIterator(cube, query).map(cellsToAggregates(measuresTable, aggregationLogics))
				.map(rawCellToNiceCell());
	}

	public static Stream<NiceCellToAggregate<List<?>>> queryToNiceCellsIterator(final ICompositeHolyCube compositeCube,
			final IAggregationQuery query) {
		return queryToRawCellsIterator(compositeCube, query).map(rawCellToNiceCell());
	}

	/**
	 * 
	 * @param definition
	 * @param query
	 * @return the {@link IAggregationLogic} available in given {@link IHolyMeasuresDefinition}, else null at given
	 *         index
	 */
	@VisibleForTesting
	static List<IAggregationLogic<?>> getAggregationLogics(final IHolyMeasuresDefinition definition,
			final IAggregationQuery query) {
		List<IMeasuredAxis> queriedMeasures = query.getMeasures();

		return queriedMeasures.stream().map(queriedMeasured -> {
			if (IStandardOperators.CELLCOUNT.equals(queriedMeasured.getOperator())) {
				// COUNT measures are implicit: they are not expressed by the measureTable as they are computed by
				// the cellSet
				LongAggregationLogic countAggregationLogic =
						new LongAggregationLogic(IHolyMeasuresDefinition.CELLCOUNT_MEASURE_INDEX,
								IStandardLongOperators.CELLCOUNT);
				return countAggregationLogic;
			} else {
				int cubeMeasureIndex = definition.findMeasureIndex(queriedMeasured);

				if (cubeMeasureIndex >= 0) {
					IAggregationLogic<?> aggregationLogic =
							definition.measures().get(cubeMeasureIndex).getAggregationLogic();
					return aggregationLogic;
				} else {
					LOGGER.debug("One is querying an unknown measuredAxis: {}", queriedMeasured);
					return null;
				}
			}
		}).collect(Collectors.toList());
	}

	public static void consumeQueryResult(IHolyCube cube,
			SimpleAggregationQuery query,
			Consumer<RawCoordinatesToBitmap> consumer) {
		IHasNavigableAxes navigableAxes = cube.getCellSet().getAxesWithCoordinates();

		List<String> wildcardAxes = query.getAxes();
		int[] axesIndexes = computeAxesIndexes(navigableAxes, wildcardAxes);

		int someUnknownAxis = Ints.indexOf(axesIndexes, -1);
		if (someUnknownAxis >= 0) {
			// We are decomposing an unknown axis
			LOGGER.debug("{} is unknown amongst {}", wildcardAxes.get(someUnknownAxis), navigableAxes.getAxes());
		} else {
			new AggregationHelper2().computeParallelNextCellRows(cube.getCellSet(),
					axesIndexes,
					cube.getFiltersBitmap(query),
					consumer);
		}
	}

	@Deprecated
	protected static <T> Function<RawCoordinatesToBitmap, T> rowsToAggregates(final IHolyCube cube,
			final IAggregationLogic<? extends T> aggregationLogic) {
		return coordinatesToRows -> aggregationLogic.aggregateTo(cube.getMeasuresTable(),
				HolyIterator.toLongIterator(coordinatesToRows.getMatchingRows().getIntIterator()),
				coordinatesToRows.getSlice());
	}

	protected static Function<RawCoordinatesToBitmap, List<?>> rowsToAggregates(final IHolyMeasuresTable measuresTable,
			final List<IAggregationLogic<?>> aggregationLogics) {
		return coordinatesToRows -> aggregationLogics.stream().map(al -> {
			if (al == null) {
				// This measure is unknown for current table
				return null;
			}
			// The same iterator is produced for N aggregationsLogic: bad-design
			LongIterator rowsIterator =
					HolyIterator.toLongIterator(coordinatesToRows.getMatchingRows().getIntIterator());
			return al.aggregateTo(measuresTable, rowsIterator, coordinatesToRows.getSlice());
		}).collect(Collectors.toList());
	}

	@Deprecated
	protected static <T> Function<RawCoordinatesToBitmap, RawCellToAggregate<T>> cellsToAggregates(final IHolyCube cube,
			final IAggregationLogic<? extends T> aggregationLogic) {
		final Function<RawCoordinatesToBitmap, T> baseFunction = rowsToAggregates(cube, aggregationLogic);

		return coordinatesToRows -> new RawCellToAggregate<T>(coordinatesToRows.getSlice(),
				baseFunction.apply(coordinatesToRows));
	}

	protected static Function<RawCoordinatesToBitmap, RawCellToAggregate<List<?>>> cellsToAggregates(
			final IHolyMeasuresTable measuresTable,
			final List<IAggregationLogic<?>> aggregationLogics) {
		final Function<RawCoordinatesToBitmap, List<?>> baseFunction =
				rowsToAggregates(measuresTable, aggregationLogics);

		return coordinatesToRows -> new RawCellToAggregate<List<?>>(coordinatesToRows.getSlice(),
				baseFunction.apply(coordinatesToRows));
	}

	protected static Function<CoordinatesRefs, NavigableMap<String, Object>> rawToNiceCoordinates() {
		return slice -> {
			NavigableMap<String, Object> coordinates = new ConcurrentSkipListMap<>();

			IHasAxesWithCoordinates axesWithCoordinates = slice.getAxesWithCoordinates();

			int[] axesIndexes = slice.getAxesIndexes();
			for (int i = 0; i < axesIndexes.length; i++) {
				int axisIndex = axesIndexes[i];
				String key = axesWithCoordinates.indexToAxis(axisIndex);
				coordinates.put(key,
						axesWithCoordinates.dereferenceCoordinate(axisIndex, slice.getCoordinatesRef()[i]));
			}

			return coordinates;
		};
	}

	@Deprecated
	protected static <T> Function<RawCellToAggregate<T>, NiceCellToAggregate<T>> rawCellToNiceCell() {
		final Function<CoordinatesRefs, NavigableMap<String, Object>> rawToNice = rawToNiceCoordinates();

		return cellToAggregate -> new NiceCellToAggregate<T>(cellToAggregate.getMeasureValue(),
				rawToNice.apply(cellToAggregate.getCoordinatesRefs()));
	}

	/**
	 * 
	 * @param cube
	 * @param query
	 * @return a {@link NavigableMap} from slice coordinates to the single queries measure
	 */
	public static NavigableMap<? extends NavigableMap<String, ?>, ?> singleMeasureToNavigableMap(final IHolyCube cube,
			final IAggregationQuery query) {
		return singleMeasureToNavigableMap(cube.asComposite(), query);
	}

	public static Optional<?> singleMeasureCell(final IHolyCube cube, final IAggregationQuery query) {
		if (!query.getAxes().isEmpty()) {
			throw new IllegalArgumentException("Can not select a single cell given wildcards: " + query.getAxes());
		}

		NavigableMap<? extends NavigableMap<?, ?>, ?> asMap = singleMeasureToNavigableMap(cube.asComposite(), query);

		if (asMap.isEmpty()) {
			return Optional.empty();
		} else if (asMap.size() == 1) {
			return Optional.of(asMap.values().iterator().next());
		} else {
			throw new IllegalStateException("We return multiple results: " + asMap);
		}
	}

	public static NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> measuresToNavigableMap(
			final ICompositeHolyCube compositeCube,
			final IAggregationQuery query) {

		// This Map will accumulate the result
		final NavigableMap<NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> coordinateToAggregate =
				new ConcurrentSkipListMap<>(NavigableMapComparator.INSTANCE);

		if (compositeCube.getNbRows() == 0L) {
			// We fork to accept unknown measuredAxis on an empty cube
			// We have a specific behavior on empty cube, as there is no cell along which we can iterate
			// We do not try resolving a relevant cell, as the filter may be complex (e.g. country IN ('FR', 'USA')) and
			// in such a case, it is unclear what should be the relevant cell to return the aggregates

			// We can only return COUNT measures, only if they are indeed available in the cube
			if (query.getMeasures().contains(ICountMeasuresConstants.COUNT_MEASURED_AXIS)) {
				// Empty is semantically different to holding only the neutral element

				long neutral = new OperatorFactory().getLongBinaryOperator(IStandardOperators.COUNT).neutralAsLong();
				coordinateToAggregate.put(new TreeMap<>(),
						Map.of(ICountMeasuresConstants.COUNT_MEASURED_AXIS, neutral));
			}
		} else {
			Stream<NiceCellToAggregate<List<?>>> niceCellToAggregate2Iterator =
					queryToNiceCellsIterator(compositeCube, query);

			// Select the single aggregate
			Stream<NiceCellToAggregate<Map<IMeasuredAxis, ?>>> niceCellToAggregateIterator =
					niceCellToAggregate2Iterator.map(l -> {
						List<?> aggregatesArray = l.getAggregate();

						Map<IMeasuredAxis, Object> aggregatesMap = new HashMap<>();
						for (int i = 0; i < query.getMeasures().size(); i++) {
							aggregatesMap.put(query.getMeasures().get(i), aggregatesArray.get(i));
						}

						return new NiceCellToAggregate<>(aggregatesMap, l.getCoordinates());
					});

			// Feed the map
			Consumer<NiceCellToAggregate<Map<IMeasuredAxis, ?>>> niceCellToAggregateConsumer = niceCellToAggregate -> {
				NavigableMap<String, Object> coordinates = niceCellToAggregate.getCoordinates();
				Map<IMeasuredAxis, ?> aggregate = niceCellToAggregate.getAggregate();
				Map<IMeasuredAxis, ?> previousAggregate = coordinateToAggregate.put(coordinates, aggregate);

				if (previousAggregate != null) {
					throw new IllegalStateException("We encountered twice the same point: " + coordinates
							+ " associated to "
							+ aggregate
							+ " and "
							+ previousAggregate);
				}
			};

			// Process the iteration: it is the terminal operation
			niceCellToAggregateIterator.iterator().forEachRemaining(niceCellToAggregateConsumer);
		}

		return coordinateToAggregate;
	}

	public static NavigableMap<? extends NavigableMap<String, ?>, ?> singleMeasureToNavigableMap(
			final ICompositeHolyCube compositeCube,
			final IAggregationQuery query) {
		if (query.getMeasures().size() == 0 || query.getMeasures().size() >= 2) {
			throw new IllegalArgumentException("Expects a single measure. Was: " + query.getMeasures());
		}

		IMeasuredAxis singleMeasure = query.getMeasures().get(0);

		NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> multipleMeasures =
				measuresToNavigableMap(compositeCube, query);

		Map<NavigableMap<String, ?>, Object> singleMeasureMap = multipleMeasures.entrySet()
				.stream()
				.filter(e -> null != e.getValue().get(singleMeasure))
				.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get(singleMeasure)));

		final NavigableMap<NavigableMap<String, ?>, Object> singleMeasureNavigableMap =
				new ConcurrentSkipListMap<>(NavigableMapComparator.INSTANCE);

		singleMeasureNavigableMap.putAll(singleMeasureMap);

		return singleMeasureNavigableMap;
	}

	protected static int[] computeWildcardIndexes(Collection<String> wildards, NavigableSet<String> allKeys) {
		int[] wildcardIndexes = new int[wildards.size()];

		int wildcardComputed = -1;
		for (String wildcard : wildards) {
			wildcardComputed++;
			// Count the number of element before this, not included
			// on the first element, we return 0

			wildcardIndexes[wildcardComputed] = indexOf(allKeys, wildcard);
		}

		return wildcardIndexes;
	}

	protected static int indexOf(NavigableSet<String> allKeys, String wildcard) {
		if (allKeys instanceof SortedSet<?>) {
			return allKeys.headSet(wildcard).size();
		} else {
			// BEWARE it is expensive to do this copy
			return indexOf(new TreeSet<>(allKeys), wildcard);
		}
	}
}
