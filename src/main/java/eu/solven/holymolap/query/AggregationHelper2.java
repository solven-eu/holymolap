package eu.solven.holymolap.query;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.codahale.metrics.Meter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.aggregate.EmptyCoordinatesRefs;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.immutable.table.RowsConsumerStatus;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

@ManagedResource
public class AggregationHelper2 {
	protected static final Logger LOGGER = LoggerFactory.getLogger(AggregationHelper2.class);

	protected final Set<RowsConsumerStatus> activeGroupBy = Sets.newConcurrentHashSet();

	/**
	 * 
	 * @param cellSet
	 * @param axesKeys
	 * @param candidateRows
	 * @param rowAggregateConsumer
	 * @return the matching rows
	 */
	public RoaringBitmap computeNextCellRows(IHolyCellMultiSet cellSet,
			int[] axesIndexes,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		if (candidateRows.isEmpty()) {
			return candidateRows;
		}

		if (axesIndexes.length == 0) {
			// All rows match as we requested the grand total
			rowAggregateConsumer.accept(new RawCoordinatesToBitmap(EmptyCoordinatesRefs.EMPTY, candidateRows));

			return candidateRows;
		}

		// Pick-up next valid row
		long rowToConsider = Util.toUnsignedLong(candidateRows.select(0));

		final long[] valuesRefs = cellSet.getTable().getCellCoordinates(rowToConsider, axesIndexes);

		RoaringBitmap matchingRowsBitmap = cellSet.getTable().getCoordinateToRows(axesIndexes, valuesRefs);

		if (!matchingRowsBitmap.contains(Ints.checkedCast(rowToConsider))) {
			throw new IllegalStateException("We should have spot at least current row");
		}

		CoordinatesRefs coordinates = new CoordinatesRefs(cellSet.getAxesWithCoordinates(), axesIndexes, valuesRefs);
		rowAggregateConsumer.accept(new RawCoordinatesToBitmap(coordinates, matchingRowsBitmap));

		return matchingRowsBitmap;
	}

	public void computeParallelNextCellRows(IHolyCellMultiSet index,
			int[] axesIndexes,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		computeParallelNextCellRows(index, axesIndexes, LongLists.emptyList(), candidateRows, rowAggregateConsumer);
	}

	protected void computeParallelNextCellRows(final IHolyCellMultiSet index,
			final int[] axesIndexes,
			final LongList valueIndexes,
			final RoaringBitmap candidateRows,
			final Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		AtomicLong nbAsyncTasks = new AtomicLong();

		RowsConsumerStatus status = new RowsConsumerStatus(candidateRows.getCardinality());
		activeGroupBy.add(status);

		try {
			computeParallelNextCellRows(index,
					axesIndexes,
					valueIndexes,
					candidateRows,
					rowAggregateConsumer,
					es,
					nbAsyncTasks,
					status);
		} finally {
			// TODO a proper mechanism to know async tasks have been consumed
			while (nbAsyncTasks.get() > 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			if (!status.isComplete()) {
				LOGGER.warn("We encountered an issue after consuming {} rows out of {}", status);
			}

			activeGroupBy.remove(status);

			es.shutdown();
		}
	}

	/**
	 * The idea of this algorithm is to pick the first row amongst not already groupedBy rows, then mask its coordinates
	 * along the wildcardAxes (hence materializing the next cell to consider), compute the aggregates for given cell,
	 * and move to the next cell.
	 * 
	 * @param cellSet
	 * @param wildcardAxes
	 * @param coordinateRefsForWildcardAxes
	 * @param candidateRows
	 * @param rowAggregateConsumer
	 * @param es
	 * @param nbTasks
	 * @param status
	 */
	protected void computeParallelNextCellRows(final IHolyCellMultiSet cellSet,
			final int[] axesIndexes,
			final LongList coordinateRefsForWildcardAxes,
			final RoaringBitmap candidateRows,
			final Consumer<RawCoordinatesToBitmap> rowAggregateConsumer,
			final Executor es,
			final AtomicLong nbTasks,
			final RowsConsumerStatus status) {
		if (axesIndexes.length == coordinateRefsForWildcardAxes.size()) {
			// We have selected a coordinate for each wildcardAxes
			rowAggregateConsumer.accept(new RawCoordinatesToBitmap(new CoordinatesRefs(cellSet.getAxesWithCoordinates(),
					axesIndexes,
					coordinateRefsForWildcardAxes.toLongArray()), candidateRows));

			// Register these rows as consumed
			status.addAsConsidered(candidateRows.getCardinality());

			return;
		} else {
			RoaringBitmap candidateRowsLeft = candidateRows;

			int previousRowToConsider = -1;

			while (!candidateRowsLeft.isEmpty()) {

				// Pick-up next valid row
				final int rowToConsider = candidateRowsLeft.select(0);

				if (rowToConsider <= previousRowToConsider) {
					throw new IllegalStateException("We are going backward");
				}

				// This array will hold the indexes of the values of the next
				// coordinate matching the selected wildcards
				{
					int axisIndex = axesIndexes[coordinateRefsForWildcardAxes.size()];

					final long valueIndex = cellSet.getTable().getCellCoordinateRef(rowToConsider, axisIndex);

					if (valueIndex == IHasAxesWithCoordinates.NOT_INDEXED) {
						String axisName = cellSet.getAxesWithCoordinates().getAxes().get(axisIndex);
						throw new IllegalStateException("We are considering a row (" + rowToConsider
								+ ") which does not contribute to key: "
								+ axisName);
					}

					final RoaringBitmap valueBitmap = cellSet.getTable().getCoordinateToRows(axisIndex, valueIndex);

					final RoaringBitmap finalCandidateRowsLeft = candidateRowsLeft;
					Runnable command = new Runnable() {

						@Override
						public void run() {
							try {
								computeOnMatchingRows(finalCandidateRowsLeft,
										valueBitmap,
										rowToConsider,
										coordinateRefsForWildcardAxes,
										valueIndex,
										cellSet,
										axesIndexes,
										rowAggregateConsumer,
										es,
										nbTasks,
										status);
							} finally {
								nbTasks.decrementAndGet();
							}
						}
					};

					nbTasks.incrementAndGet();
					if (candidateRowsLeft.getCardinality() > 1024) {
						// Fork only if there is enough rows to consider

						es.execute(command);
					} else {
						command.run();
					}

					{
						RoaringBitmap notMatchingRowsBitmap = RoaringBitmap.andNot(candidateRowsLeft, valueBitmap);
						if (notMatchingRowsBitmap.contains(rowToConsider)) {
							throw new IllegalStateException("We should have rejected current row");
						}

						candidateRowsLeft = notMatchingRowsBitmap;
					}
				}
			}
		}
	}

	protected void computeOnMatchingRows(RoaringBitmap candidateRowsLeft,
			RoaringBitmap valueBitmap,
			long rowToConsider,
			LongList valueIndexes,
			long valueIndex,
			IHolyCellMultiSet index,
			int[] axesIndexes,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer,
			Executor es,
			AtomicLong nbAsyncTasks,
			RowsConsumerStatus status) {
		RoaringBitmap matchingRowsBitmap = RoaringBitmap.and(candidateRowsLeft, valueBitmap);

		if (!matchingRowsBitmap.contains(Ints.checkedCast(rowToConsider))) {
			throw new IllegalStateException("We should have spot at least current row");
		}

		LongList matchingValueIndexes = new LongArrayList(valueIndexes);
		matchingValueIndexes.add(valueIndex);
		computeParallelNextCellRows(index,
				axesIndexes,
				matchingValueIndexes,
				matchingRowsBitmap,
				rowAggregateConsumer,
				es,
				nbAsyncTasks,
				status);
	}

	/**
	 * 
	 * @param index
	 * @param wildcards
	 * @param rows
	 * @param consumer
	 * @return rows left to process
	 */
	public RoaringBitmap consumeNextCellRows(final IHolyCellMultiSet index,
			final int[] axesIndexes,
			final RoaringBitmap rows,
			Consumer<RawCoordinatesToBitmap> consumer) {
		if (rows.isEmpty()) {
			return rows;
		} else {
			RoaringBitmap matchingRows = computeNextCellRows(index, axesIndexes, rows, consumer);

			// LOGGER.debug("We consumed {} rows out of {}",
			// matchingRows.getCardinality(), rows.getCardinality());

			// Do not consider the matching rows for future
			// aggregation
			// TODO: could we modify rows in place?
			return RoaringBitmap.andNot(rows, matchingRows);
		}
	}

	public Iterator<RawCoordinatesToBitmap> asIterator(final IHolyCellMultiSet index,
			final int[] axesIndexes,
			final RoaringBitmap rows) {
		final AtomicReference<RawCoordinatesToBitmap> nextAggregate = new AtomicReference<>();

		final Consumer<RawCoordinatesToBitmap> consumer = newValue -> {
			nextAggregate.set(newValue);
		};

		final int initialNbRows = rows.getCardinality();

		final Meter meter = new Meter();
		final Meter aggregateMeter = new Meter();

		final AtomicLong start = new AtomicLong(System.currentTimeMillis());

		return new AbstractIterator<RawCoordinatesToBitmap>() {

			protected RoaringBitmap leftRows = rows;

			@Override
			protected RawCoordinatesToBitmap computeNext() {
				if (leftRows.isEmpty()) {
					return endOfData();
				} else {
					int nbRowsBefore = leftRows.getCardinality();

					// Do not consider the matching rows for future
					// aggregation
					leftRows = consumeNextCellRows(index, axesIndexes, leftRows, consumer);

					int nbLeftRows = leftRows.getCardinality();

					// N underlying cells
					meter.mark(nbRowsBefore - nbLeftRows);
					// For a single aggregate
					aggregateMeter.mark();

					long now = System.currentTimeMillis();
					if (now > start.get() + TimeUnit.SECONDS.toMillis(1)) {
						LOGGER.debug("We consumed {} rows out of {} at rate {}rows/sec, {}agg/sec",
								initialNbRows - nbLeftRows,
								initialNbRows,
								meter.getMeanRate(),
								aggregateMeter.getMeanRate());
						start.set(now);
					}

					RawCoordinatesToBitmap actualNext = nextAggregate.get();

					if (actualNext == null) {
						// There is no more rows
						return endOfData();
					} else {
						return actualNext;
					}
				}
			}

		};
	}

	public Stream<RawCoordinatesToBitmap> asStream(final IHolyCellMultiSet index,
			final int[] axesIndexes,
			final RoaringBitmap rows) {
		Iterator<RawCoordinatesToBitmap> iterator = asIterator(index, axesIndexes, rows);
		// Immutable as the underlying cube is immutable
		Spliterator<RawCoordinatesToBitmap> spliterator =
				Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE);
		return StreamSupport.stream(spliterator, false);
	}
}
