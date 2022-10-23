package eu.solven.holymolap.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

import eu.solven.holymolap.aggregate.NiceCellToAggregate;
import eu.solven.holymolap.aggregate.RawCellToAggregate;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.aggregation.IAggregationLogic;
import eu.solven.holymolap.measures.aggregation.LongAggregationLogic;
import eu.solven.holymolap.measures.operator.IOperatorFactory;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.utils.HolyIterator;

public class AggregateHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(AggregateHelper.class);

	public static Iterator<RawCoordinatesToBitmap> queryToCellsIterator(final IHolyCube cube,
			final IAggregationQuery query) {
		return new AggregationHelper2().nextCellRows(cube.getCellSet(), query.getAxes(), cube.getFiltersBitmap(query));
	}

	public static void consumeQueryResult(IHolyCube cube,
			SimpleAggregationQuery query,
			Consumer<RawCoordinatesToBitmap> consumer) {
		new AggregationHelper2().computeParallelNextCellRows(cube.getCellSet(),
				query.getAxes(),
				cube.getFiltersBitmap(query),
				consumer);
	}

	protected static <T> Function<RawCoordinatesToBitmap, T> rowsToAggregates(final IHolyCube cube,
			final IAggregationLogic<? extends T> aggregationLogic) {
		return coordinatesToRows -> aggregationLogic.aggregateTo(cube.getMeasuresTable(),
				HolyIterator.toLongIterator(coordinatesToRows.getMatchingRows().getIntIterator()),
				coordinatesToRows.getCoordinateRefs());
	}

	protected static <T> Function<RawCoordinatesToBitmap, RawCellToAggregate<T>> cellsToAggregates(final IHolyCube cube,
			final IAggregationLogic<? extends T> aggregationLogic) {
		final Function<RawCoordinatesToBitmap, T> baseFunction = rowsToAggregates(cube, aggregationLogic);

		return coordinatesToRows -> new RawCellToAggregate<T>(coordinatesToRows.getCoordinateRefs(),
				baseFunction.apply(coordinatesToRows));
	}

	protected static Function<long[], NavigableMap<String, Object>> rawToNiceCoordinates(final IHolyCube cube,
			final IAggregationQuery query) {
		IHasAxesWithCoordinates axesWithCoordinates = cube.getCellSet().getAxesWithCoordinates();
		final int[] wildcardIndexes = computeWildcardIndexes(query.getAxes(), axesWithCoordinates.axes());

		return valueIndexes -> {
			NavigableMap<String, Object> coordinates = new ConcurrentSkipListMap<>();

			for (int i = 0; i < wildcardIndexes.length; i++) {
				int axisIndex = wildcardIndexes[i];
				String key = axesWithCoordinates.indexToAxis(axisIndex);
				coordinates.put(key, axesWithCoordinates.dereferenceCoordinate(axisIndex, valueIndexes[i]));
			}

			return coordinates;
		};
	}

	protected static <T> Function<RawCellToAggregate<T>, NiceCellToAggregate<T>> rawCellToNiceCell(final IHolyCube cube,
			final IAggregationQuery query) {
		final Function<long[], NavigableMap<String, Object>> rawToNice = rawToNiceCoordinates(cube, query);

		return cellToAggregate -> new NiceCellToAggregate<T>(cellToAggregate.getMeasureValue(),
				rawToNice.apply(cellToAggregate.getCoordinatesRef()));
	}

	public static NavigableMap<? extends NavigableMap<?, ?>, ?> cumulateInNavigableMap(final IHolyCube cube,
			final IAggregationQuery query) {
		// This Map will accumulate the result
		final NavigableMap<NavigableMap<?, ?>, Object> coordinateToAggregate =
				new ConcurrentSkipListMap<NavigableMap<?, ?>, Object>(NavigableMapComparator.INSTANCE);

		if (cube.getNbRows() == 0L) {
			// We fork to accept unknown measuredAxis on an empty cube
			// We have a specific behavior on empty cube, as there is no cell along which we can iterate
			// We do not try resolving a relevant cell, as the filter may be complex (e.g. country IN ('FR', 'USA')) and
			// in such a case, it is unclear what should be the relevant cell to return the aggregates

			// We return a return only if querying COUNT(*)
			if (query.getMeasures().contains(ICountMeasuresConstants.COUNT_MEASURED_AXIS)) {
				// Empty is semantically different to holding only the neutral element

				long neutral = new OperatorFactory().getLongBinaryOperator(IOperatorFactory.COUNT).neutralAsLong();
				coordinateToAggregate.put(new TreeMap<>(), neutral);
			}
		} else {
			List<IMeasuredAxis> queriedMeasures = query.getMeasures();

			// groupBy measures by measuredAxis
			Multimap<String, IAggregationLogic<?>> aggregatedToLogic =
					MultimapBuilder.treeKeys().arrayListValues().build();

			IHolyMeasuresDefinition definition = cube.getMeasuresTable().getDefinition();
			queriedMeasures.forEach(queriedMeasured -> {
				if (IOperatorFactory.CELLCOUNT.equals(queriedMeasured.getOperator())) {
					// COUNT measures are implicit: they are not expressed by the measureTable as they are computed by
					// the cellSet
					LongAggregationLogic countAggregationLogic =
							new LongAggregationLogic(IHolyMeasuresDefinition.CELLCOUNT_MEASURE_INDEX,
									IStandardOperators.CELLCOUNT);
					aggregatedToLogic.put(queriedMeasured.getAxis(), countAggregationLogic);
				} else {
					int cubeMeasureIndex = definition.findMeasureIndex(queriedMeasured);

					if (cubeMeasureIndex >= 0) {
						IAggregationLogic<?> aggregationLogic =
								definition.measures().get(cubeMeasureIndex).getAggregationLogic();
						aggregatedToLogic.put(queriedMeasured.getAxis(), aggregationLogic);
					} else {
						LOGGER.debug("One is quewrying an unknown measuredAxis: {}", queriedMeasured);
					}
				}
			});

			if (aggregatedToLogic.size() >= 2) {
				throw new UnsupportedOperationException(
						"TODO Manage multiple aggregation in a single query: " + aggregatedToLogic);
			} else if (aggregatedToLogic.isEmpty()) {
				LOGGER.debug("Not a single measure");
			} else {
				IAggregationLogic<?> aggregationLogic = aggregatedToLogic.values().iterator().next();

				Iterator<RawCoordinatesToBitmap> cellsIterator = queryToCellsIterator(cube, query);

				Function<RawCoordinatesToBitmap, RawCellToAggregate<Object>> cellToAggregates =
						cellsToAggregates(cube, aggregationLogic);
				Function<RawCellToAggregate<Object>, NiceCellToAggregate<Object>> rawCellToNiceCell =
						rawCellToNiceCell(cube, query);

				Iterator<NiceCellToAggregate<Object>> niceCellToAggregateIterator =
						Iterators.transform(cellsIterator, Functions.compose(rawCellToNiceCell, cellToAggregates));

				Consumer<NiceCellToAggregate<Object>> niceCellToAggregateConsumer = niceCellToAggregate -> {
					NavigableMap<String, Object> coordinates = niceCellToAggregate.getCoordinates();
					Object aggregate = niceCellToAggregate.getAggregate();
					Object previousAggregate = coordinateToAggregate.put(coordinates, aggregate);

					if (previousAggregate != null) {
						throw new IllegalStateException("We encountered twice the same point: " + coordinates
								+ " associated to "
								+ aggregate
								+ " and "
								+ previousAggregate);
					}
				};

				pushToResultMap(niceCellToAggregateIterator, niceCellToAggregateConsumer);
			}
		}

		return coordinateToAggregate;
	}

	public static <T> void pushToResultMap(Iterator<T> iterator, Consumer<T> consumer) {
		while (iterator.hasNext()) {
			consumer.accept(iterator.next());
		}
	}

	public static int[] computeWildcardIndexes(Collection<String> wildards, NavigableSet<String> allKeys) {
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

	public static int indexOf(NavigableSet<String> allKeys, String wildcard) {
		if (allKeys instanceof SortedSet<?>) {
			return allKeys.headSet(wildcard).size();
		} else {
			// BEWARE it is expensive to do this copy
			return indexOf(new TreeSet<>(allKeys), wildcard);
		}
	}
}
