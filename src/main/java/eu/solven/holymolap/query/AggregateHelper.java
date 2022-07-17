package eu.solven.holymolap.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterators;

import eu.solven.holymolap.PositionIndexBuilder;
import eu.solven.holymolap.aggregate.NicePointToAggregates;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.aggregate.RawPointToAggregates;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import javolution.util.function.Consumer;

public class AggregateHelper {

	public static Iterator<RawCoordinatesToBitmap> queryToAggregateIterator(final IHolyCube cube,
			final IAggregationQuery query) {
		return new PositionIndexBuilder()
				.nextCellRows(cube.getIndex(), query.getColumns(), cube.getFiltersBitmap(query));
	}

	public static void consumeQueryResult(IHolyCube cube,
			SimpleAggregationQuery query,
			Consumer<RawCoordinatesToBitmap> consumer) {
		new PositionIndexBuilder().computeParallelNextCellRows(cube.getIndex(),
				query.getColumns(),
				cube.getFiltersBitmap(query),
				consumer);
	}

	protected static <T> Function<RawCoordinatesToBitmap, T> rowsToAggregates(final IHolyCube cube,
			final IAggregationLogic<T> aggregationLogic) {
		return coordinatesToRows -> aggregationLogic
				.aggregateTo(cube, coordinatesToRows.matchingRows.getIntIterator(), coordinatesToRows.valueIndexes);
	}

	protected static <S, T> Function<RawCoordinatesToBitmap, RawPointToAggregates<T>> rowsToAggregates2(
			final IHolyCube cube,
			final IAggregationQuery query,
			final IAggregationLogic<T> aggregationLogic) {
		final Function<RawCoordinatesToBitmap, T> baseFunction = rowsToAggregates(cube, aggregationLogic);

		return coordinatesToRows -> new RawPointToAggregates<T>(baseFunction.apply(coordinatesToRows),
				coordinatesToRows.valueIndexes);
	}

	protected static Function<int[], NavigableMap<String, Object>> rawToNiceCoordinates(final IHolyCube cube,
			final IAggregationQuery query) {
		final int[] wildcardIndexes =
				computeWildcardIndexes(query.getColumns(), new TreeSet<Object>(cube.getIndex().keySet()));

		return valueIndexes -> {
			NavigableMap<String, Object> coordinates = new ConcurrentSkipListMap<>();

			for (int i = 0; i < wildcardIndexes.length; i++) {
				String key = cube.indexToColumn(wildcardIndexes[i]);
				coordinates.put(key, cube.getIndex().convertValueIndexToValue(key, valueIndexes[i]));
			}

			return coordinates;
		};
	}

	protected static <T> Function<RawPointToAggregates<T>, NicePointToAggregates<T>> rawToNicePoint(
			final IHolyCube cube,
			final IAggregationQuery query) {
		final Function<int[], NavigableMap<String, Object>> baseFunction = rawToNiceCoordinates(cube, query);

		return pointToAggregates -> new NicePointToAggregates<T>(pointToAggregates.keyToAggregates,
				baseFunction.apply(pointToAggregates.valueIndexes));
	}

	public static <T> NavigableMap<? extends NavigableMap<?, ?>, ? extends T> cumulateInNavigableMap(
			final IHolyCube cube,
			final IAggregationQuery query,
			final IAggregationLogic<T> aggregationLogic) {

		Iterator<RawCoordinatesToBitmap> rawIterator = queryToAggregateIterator(cube, query);

		Function<RawCoordinatesToBitmap, RawPointToAggregates<T>> rowsToAggregates2 =
				rowsToAggregates2(cube, query, aggregationLogic);
		Function<RawPointToAggregates<T>, NicePointToAggregates<T>> rawToNicePoint = rawToNicePoint(cube, query);

		Iterator<NicePointToAggregates<T>> niceIterator =
				Iterators.transform(rawIterator, Functions.compose(rawToNicePoint, rowsToAggregates2));

		// This Map will accumulate the result
		final NavigableMap<NavigableMap<?, ?>, T> coordinateToAggregate =
				new ConcurrentSkipListMap<NavigableMap<?, ?>, T>(NavigableMapComparator.INSTANCE);

		Consumer<NicePointToAggregates<T>> pointAggregatesConsumer = pointToAggregates -> {
			T previous = coordinateToAggregate.put(pointToAggregates.keyToValue, pointToAggregates.keyToAggregates);

			if (previous != null) {
				throw new IllegalStateException("We encountered twice the same point: " + pointToAggregates.keyToValue
						+ " associated to "
						+ pointToAggregates.keyToAggregates
						+ " and "
						+ previous);
			}
		};

		pushToResultMap(niceIterator, pointAggregatesConsumer);

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
