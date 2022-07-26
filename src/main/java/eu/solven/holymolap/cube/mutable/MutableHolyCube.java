package eu.solven.holymolap.cube.mutable;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.query.operator.OperatorFactory;
import eu.solven.holymolap.stable.v1.IAggregatedAxis;
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
	final Map<IAggregatedAxis, IMutableDoubleAggregateColumn> aggregationToColumn;
	// The Set of axes with at least one aggregation
	final Set<String> aggregatedAxes;

	final List<String> orderedAxes;
	final Map<String, IMutableAxisColumn> axisToColumn;

	// A cell is identified by a tuple of coordinateIndexes along axes
	// cellToRow can be recomputed from axisToColumn, but it is necessary to quickly identify to which row an input has
	// to be aggregated into.
	final Object2IntMap<IntList> cellToRow;

	final AtomicLong brokenRows = new AtomicLong();

	protected MutableHolyCube(Map<IAggregatedAxis, IMutableDoubleAggregateColumn> aggregationToColumn,
			List<String> orderedAxis,
			Map<String, IMutableAxisColumn> axisToColumn,
			Object2IntMap<IntList> cellToRow) {
		this.aggregationToColumn = aggregationToColumn;
		this.aggregatedAxes = aggregationToColumn.keySet().stream().map(aa -> aa.getAxis()).collect(Collectors.toSet());

		this.orderedAxes = orderedAxis;
		this.axisToColumn = axisToColumn;
		if (orderedAxis.size() != axisToColumn.size()) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		} else if (!orderedAxis.stream().distinct().collect(Collectors.toSet()).equals(axisToColumn.keySet())) {
			throw new IllegalArgumentException("Inconsistency " + orderedAxis + " vs " + axisToColumn.keySet());
		}

		this.cellToRow = cellToRow;
		if (cellToRow.defaultReturnValue() != IMutableAxisDictionary.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + cellToRow.defaultReturnValue());
		}
	}

	/**
	 * The {@link IAggregatedAxis} are provided from the beginning, as they will be
	 * 
	 * @param aggregations
	 */
	public MutableHolyCube(Set<IAggregatedAxis> aggregations) {
		this(prepareAggregationColumns(aggregations),
				prepareOrderedAxes(),
				new ConcurrentHashMap<>(),
				prepareCellToRow());
	}

	private static Object2IntOpenHashMap<IntList> prepareCellToRow() {
		Object2IntOpenHashMap<IntList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	private static List<String> prepareOrderedAxes() {
		// We chose a thread-safe collection not to synchronize any read from it.
		// We expect a low number of writes, hence COpyOnWrite is OK.
		return new CopyOnWriteArrayList<String>();
	}

	private static Map<IAggregatedAxis, IMutableDoubleAggregateColumn> prepareAggregationColumns(
			Set<IAggregatedAxis> aggregations) {
		OperatorFactory operatorFactory = new OperatorFactory();

		return aggregations.stream()
				.collect(Collectors.toMap(a -> a, a -> provisionAggregateColumn(operatorFactory, a)));
	}

	protected static MutableAggregateColumn provisionAggregateColumn(OperatorFactory operatorFactory,
			IAggregatedAxis a) {
		return new MutableAggregateColumn(operatorFactory.getDoubleBinaryOperator(a.getOperator()));
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
			if (aggregatedAxes.contains(axis)) {
				aggregated.put(axis, coordinate);
			} else {
				groupBy.put(axis, coordinate);
			}
		});

		acceptRowToCell(aggregated, groupBy);
	}

	@Override
	public void acceptRowToCell(Map<String, ?> aggregated, Map<String, ?> groupBy) {
		SetView<String> invalidGroupBys = Sets.intersection(aggregatedAxes, groupBy.keySet());
		if (!invalidGroupBys.isEmpty()) {
			throw new IllegalArgumentException("Can not groupBy aggregated keys: " + invalidGroupBys);
		}

		if (groupBy.size() > orderedAxes.size()) {
			// This row introduces a new axes
			Set<String> additionalAxes = new LinkedHashSet<>(groupBy.keySet());
			additionalAxes.removeAll(axisToColumn.keySet());

			orderedAxes.addAll(additionalAxes);
			additionalAxes.forEach(axis -> axisToColumn.putIfAbsent(axis, new MutableAxisColumn()));
		}

		// Append the coordinates for the known axes
		IntList cellCoordinates = indexGroupBy(groupBy);

		int cellIndex;
		int newCellIndex;
		synchronized (cellToRow) {
			int localCellIndex = cellToRow.getInt(cellCoordinates);
			if (localCellIndex >= 0) {
				cellIndex = localCellIndex;
				newCellIndex = IMutableAxisDictionary.NO_COORDINATE_INDEX;
			} else {
				newCellIndex = cellToRow.size();
				cellIndex = newCellIndex;
				cellToRow.put(cellCoordinates, newCellIndex);
			}
		}

		if (newCellIndex >= 0) {
			// Register this new cell
			for (int axisIndex = 0; axisIndex < cellCoordinates.size(); axisIndex++) {
				axisToColumn.get(orderedAxes.get(axisIndex)).appendCoordinateIndex(cellCoordinates.getInt(axisIndex));
			}
		}

		aggregationToColumn.forEach((aggregatedAxis, column) -> {
			String axis = aggregatedAxis.getAxis();
			Object contribution = aggregated.get(axis);

			if (contribution instanceof Number) {
				column.aggregateRow(cellIndex, ((Number) contribution).doubleValue());
			} else if (contribution != null) {
				brokenRows.incrementAndGet();
			}
		});
	}

	protected IntList indexGroupBy(Map<String, ?> groupBy) {
		IntList cellCoordinates = new IntArrayList(groupBy.size());

		// Register the coordinate
		orderedAxes.stream().forEach(axis -> {
			Object coordinate = groupBy.get(axis);

			int coordinateIndex;
			if (coordinate == null || IMutableAxisDictionary.NO_COORDINATE == coordinate) {
				coordinateIndex = IMutableAxisDictionary.NO_COORDINATE_INDEX;
			} else {
				coordinateIndex = axisToColumn.get(axis).getCoordinateToIndex().getIndexMayAppend(coordinate);
			}

			cellCoordinates.add(coordinateIndex);
		});

		// Remove the trailing no_coordinates, to prevent array differing only by trailing no_coordinates
		while (!cellCoordinates.isEmpty()
				&& cellCoordinates.getInt(cellCoordinates.size() - 1) == IMutableAxisDictionary.NO_COORDINATE_INDEX) {
			cellCoordinates.removeInt(cellCoordinates.size() - 1);
		}

		return cellCoordinates;
	}

}
