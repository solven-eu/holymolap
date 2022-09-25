package eu.solven.holymolap.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;

import eu.solven.holymolap.aggregate.NicePointToAggregates;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.aggregate.RawPointToAggregates;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.utils.HolyIterator;

public class AggregateHelper {

	public static Iterator<RawCoordinatesToBitmap> queryToAggregateIterator(final IHolyCube cube,
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
				coordinatesToRows.getValueRefs());
	}

	protected static <T> Function<RawCoordinatesToBitmap, RawPointToAggregates<T>> rowsToAggregates2(
			final IHolyCube cube,
			final IAggregationLogic<? extends T> aggregationLogic) {
		final Function<RawCoordinatesToBitmap, T> baseFunction = rowsToAggregates(cube, aggregationLogic);

		return coordinatesToRows -> new RawPointToAggregates<T>(coordinatesToRows.getValueRefs(),
				baseFunction.apply(coordinatesToRows));
	}

	protected static Function<long[], NavigableMap<String, Object>> rawToNiceCoordinates(final IHolyCube cube,
			final IAggregationQuery query) {
		IHasAxesWithCoordinates axesWithCoordinates = cube.getCellSet().getAxesWithCoordinates();
		final int[] wildcardIndexes =
				computeWildcardIndexes(query.getAxes(), new TreeSet<Object>(axesWithCoordinates.axes()));

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

	protected static <T> Function<RawPointToAggregates<T>, NicePointToAggregates<T>> rawToNicePoint(
			final IHolyCube cube,
			final IAggregationQuery query) {
		final Function<long[], NavigableMap<String, Object>> rawToNice = rawToNiceCoordinates(cube, query);

		return pointToAggregates -> new NicePointToAggregates<T>(pointToAggregates.getMeasureValue(),
				rawToNice.apply(pointToAggregates.getCoordinatesRef()));
	}

	public static NavigableMap<? extends NavigableMap<?, ?>, ?> cumulateInNavigableMap(final IHolyCube cube,
			final IAggregationQuery query) {
		// This Map will accumulate the result
		final NavigableMap<NavigableMap<?, ?>, Object> coordinateToAggregate =
				new ConcurrentSkipListMap<NavigableMap<?, ?>, Object>(NavigableMapComparator.INSTANCE);

		if (cube.getNbRows() == 0L) {
			// This enables accepting unknown measuredAxis on an empty cube

			// Empty is semantically different to holding only the neutral element
			// String operator = Iterables.getOnlyElement(query.getAggregations()).getOperator();
			// double neutral = new OperatorFactory().getDoubleBinaryOperator(operator).neutral();
			//
			// coordinateToAggregate.put(new TreeMap<>(), neutral);
		} else {
			List<IMeasuredAxis> aggregations = query.getAggregations();
			SetMultimap<String, String> aggregatedToOperators = MultimapBuilder.treeKeys().hashSetValues().build();
			aggregations.forEach(aggregatedAxis -> aggregatedToOperators.put(aggregatedAxis.getAxis(),
					aggregatedAxis.getOperator()));

			// OperatorFactory operatorFactory = new OperatorFactory();
			Multimap<String, IAggregationLogic<?>> aggregatedToLogic =
					MultimapBuilder.treeKeys().arrayListValues().build();

			aggregations.forEach(aggregatesAxis -> {
				aggregatedToLogic.put(aggregatesAxis.getAxis(),
						SingleColumnAggregationLogic.search(cube.getMeasuresTable().getDefinition(), aggregatesAxis));
			});

			if (aggregatedToLogic.size() != 1) {
				throw new UnsupportedOperationException("TODO Manage multiple aggregation in a single query");
			}
			IAggregationLogic<?> aggregationLogic = aggregatedToLogic.values().iterator().next();

			Iterator<RawCoordinatesToBitmap> rawIterator = queryToAggregateIterator(cube, query);

			Function<RawCoordinatesToBitmap, RawPointToAggregates<Object>> rowsToAggregates2 =
					rowsToAggregates2(cube, aggregationLogic);
			Function<RawPointToAggregates<Object>, NicePointToAggregates<Object>> rawToNicePoint =
					rawToNicePoint(cube, query);

			Iterator<NicePointToAggregates<Object>> niceIterator =
					Iterators.transform(rawIterator, Functions.compose(rawToNicePoint, rowsToAggregates2));

			Consumer<NicePointToAggregates<Object>> pointAggregatesConsumer = pointToAggregates -> {
				NavigableMap<String, Object> keyToValue = pointToAggregates.getKeyToValue();
				Object keyToAggregates = pointToAggregates.getKeyToAggregates();
				Object previous = coordinateToAggregate.put(keyToValue, keyToAggregates);

				if (previous != null) {
					throw new IllegalStateException("We encountered twice the same point: " + keyToValue
							+ " associated to "
							+ keyToAggregates
							+ " and "
							+ previous);
				}
			};

			pushToResultMap(niceIterator, pointAggregatesConsumer);
		}

		return coordinateToAggregate;
	}

	public static <T> void pushToResultMap(Iterator<T> iterator, Consumer<T> consumer) {
		while (iterator.hasNext()) {
			consumer.accept(iterator.next());
		}
	}

	public static int[] computeWildcardIndexes(Collection<String> wildards, SortedSet<Object> allKeys) {
		int[] wildcardIndexes = new int[wildards.size()];

		int wildcardComputed = -1;
		for (Object wildcard : wildards) {
			wildcardComputed++;
			// Count the number of element before this, not included
			// on the first element, we return 0
			wildcardIndexes[wildcardComputed] = indexOf(allKeys, wildcard);
		}

		return wildcardIndexes;
	}

	public static int indexOf(SortedSet<Object> allKeys, Object wildcard) {
		return allKeys.headSet(wildcard).size();
	}
}
