package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import eu.solven.holymolap.IHolyCube;
import eu.solven.holymolap.PositionIndexBuilder;
import eu.solven.holymolap.aggregate.NicePointToAggregates;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.aggregate.RawPointToAggregates;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import javolution.util.function.Consumer;

public class AggregateHelper {
	public static RoaringBitmap prepareQueryBitmap(final IHolyCube cube, final AggregateQuery query) {
		List<Collection<? extends RoaringBitmap>> andBitmaps = new ArrayList<>();

		// All wildcards need to be present on each row
		if (!query.wildcards.isEmpty()) {
			andBitmaps.add(cube.getKeyBitmaps(query.wildcards));
		}

		// Keep rows matching all filters
		if (!query.filters.isEmpty()) {
			andBitmaps.add(cube.getValueBitmaps(query.filters));
		}

		// Keep only rows which hold a value for at least one measure
		// List<RoaringBitmap> orBitmaps = new ArrayList<>();
		{
			Collection<? extends RoaringBitmap> bitmaps = cube.getKeyBitmaps(query.aggregatedKeys);

			// If bitmaps is empty, the OR returns an Empty bitmap: OK
			RoaringBitmap atLeastOneMeasureBitmap = FastAggregation.or(bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
			// orBitmaps.addAll(bitmaps);

			andBitmaps.add(Collections.singleton(atLeastOneMeasureBitmap));
		}

		// https://github.com/lemire/RoaringBitmap/issues/39
		List<RoaringBitmap> bitmaps = Lists.newArrayList(Iterables.concat(andBitmaps).iterator());
		RoaringBitmap asOneBitmap = bitmaps.size() == 1 ? bitmaps.get(0) : FastAggregation.and(bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));

		return asOneBitmap;
	}

	public static Iterator<RawCoordinatesToBitmap> queryToAggregateIterator(final IHolyCube cube, final AggregateQuery query) {
		return new PositionIndexBuilder().nextCellRows(cube.getIndex(), query.wildcards, prepareQueryBitmap(cube, query));
	}

	public static void consumeQueryResult(IHolyCube cube, AggregateQuery query, Consumer<RawCoordinatesToBitmap> consumer) {
		new PositionIndexBuilder().computeParallelNextCellRows(cube.getIndex(), new ArrayList<>(query.wildcards), prepareQueryBitmap(cube, query),
				consumer);
	}

	protected static <T> Function<RawCoordinatesToBitmap, T> rowsToAggregates(final IHolyCube cube, final AggregationLogic<T> aggregationLogic) {
		return new Function<RawCoordinatesToBitmap, T>() {

			@Override
			public T apply(RawCoordinatesToBitmap coordinatesToRows) {
				return aggregationLogic.aggregateTo(cube, coordinatesToRows.matchingRows.getIntIterator(), coordinatesToRows.valueIndexes);
			}
		};
	}

	protected static <S, T> Function<RawCoordinatesToBitmap, RawPointToAggregates<T>> rowsToAggregates2(final IHolyCube cube,
			final AggregateQuery query, final AggregationLogic<T> aggregationLogic) {
		final Function<RawCoordinatesToBitmap, T> baseFunction = rowsToAggregates(cube, aggregationLogic);

		return new Function<RawCoordinatesToBitmap, RawPointToAggregates<T>>() {

			@Override
			public RawPointToAggregates<T> apply(RawCoordinatesToBitmap coordinatesToRows) {
				return new RawPointToAggregates<T>(baseFunction.apply(coordinatesToRows), coordinatesToRows.valueIndexes);
			}
		};
	}

	protected static Function<int[], NavigableMap<Object, Object>> rawToNiceCoordinates(final IHolyCube cube, final AggregateQuery query) {
		final int[] wildcardIndexes = computeWildcardIndexes(query.wildcards, new TreeSet<Object>(cube.getIndex().keySet()));

		return new Function<int[], NavigableMap<Object, Object>>() {

			@Override
			public NavigableMap<Object, Object> apply(int[] valueIndexes) {
				NavigableMap<Object, Object> coordinates = new ConcurrentSkipListMap<>();

				for (int i = 0; i < wildcardIndexes.length; i++) {
					Object key = cube.convertKeyIndexToKey(wildcardIndexes[i]);
					coordinates.put(key, cube.getIndex().convertValueIndexToValue(key, valueIndexes[i]));
				}

				return coordinates;
			}
		};
	}

	protected static <T> Function<RawPointToAggregates<T>, NicePointToAggregates<T>> rawToNicePoint(final IHolyCube cube,
			final AggregateQuery query) {
		final Function<int[], NavigableMap<Object, Object>> baseFunction = rawToNiceCoordinates(cube, query);

		return new Function<RawPointToAggregates<T>, NicePointToAggregates<T>>() {

			@Override
			public NicePointToAggregates<T> apply(RawPointToAggregates<T> pointToAggregates) {
				return new NicePointToAggregates<T>(pointToAggregates.keyToAggregates, baseFunction.apply(pointToAggregates.valueIndexes));
			}
		};
	}

	public static <T> NavigableMap<? extends NavigableMap<?, ?>, ? extends T> cumulateInNavigableMap(final IHolyCube cube,
			final AggregateQuery query, final AggregationLogic<T> aggregationLogic) {

		Iterator<RawCoordinatesToBitmap> rawIterator = queryToAggregateIterator(cube, query);

		Function<RawCoordinatesToBitmap, RawPointToAggregates<T>> rowsToAggregates2 = rowsToAggregates2(cube, query, aggregationLogic);
		Function<RawPointToAggregates<T>, NicePointToAggregates<T>> rawToNicePoint = rawToNicePoint(cube, query);

		Iterator<NicePointToAggregates<T>> niceIterator = Iterators.transform(rawIterator, Functions.compose(rawToNicePoint, rowsToAggregates2));

		// This Map will accumulate the result
		final NavigableMap<NavigableMap<?, ?>, T> coordinateToAggregate = new ConcurrentSkipListMap<NavigableMap<?, ?>, T>(
				NavigableMapComparator.INSTANCE);

		Consumer<NicePointToAggregates<T>> pointAggregatesConsumer = new Consumer<NicePointToAggregates<T>>() {

			@Override
			public void accept(NicePointToAggregates<T> pointToAggregates) {
				T previous = coordinateToAggregate.put(pointToAggregates.keyToValue, pointToAggregates.keyToAggregates);

				if (previous != null) {
					throw new IllegalStateException("We encountered twice the same point: " + pointToAggregates.keyToValue + " associated to "
							+ pointToAggregates.keyToAggregates + " and " + previous);
				}
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

	public static int[] computeWildcardIndexes(Set<?> wildards, SortedSet<Object> allKeys) {
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
