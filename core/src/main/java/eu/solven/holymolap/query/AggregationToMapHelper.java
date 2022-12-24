package eu.solven.holymolap.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.holymolap.aggregate.NiceCellToAggregate;
import eu.solven.holymolap.comparable.NavigableMapComparator;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class AggregationToMapHelper {
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

	public static NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> measuresToNavigableMap(
			final ICompositeHolyCube compositeCube,
			final IAggregationQuery query) {
		Stream<NiceCellToAggregate<List<?>>> niceCellToAggregate2Iterator =
				AggregationHelper.toNiceCells(compositeCube, query);

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

		// This Map will accumulate the result
		final NavigableMap<NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> coordinateToAggregate =
				new ConcurrentSkipListMap<>(NavigableMapComparator.INSTANCE);

		// Feed the map
		Consumer<NiceCellToAggregate<Map<IMeasuredAxis, ?>>> niceCellToAggregateConsumer = niceCellToAggregate -> {
			NavigableMap<String, Object> coordinates = niceCellToAggregate.getCoordinates();
			Map<IMeasuredAxis, ?> aggregate = niceCellToAggregate.getAggregate();
			Map<IMeasuredAxis, ?> previousAggregate = coordinateToAggregate.put(coordinates, aggregate);

			if (previousAggregate != null) {
				// This is a bug in HolyMolap
				throw new IllegalStateException("We encountered twice the same point: " + coordinates
						+ " associated to "
						+ aggregate
						+ " and "
						+ previousAggregate);
			}
		};

		// Process the iteration: it is the terminal operation
		niceCellToAggregateIterator.iterator().forEachRemaining(niceCellToAggregateConsumer);

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
}
