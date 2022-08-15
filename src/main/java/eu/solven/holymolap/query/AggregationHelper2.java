package eu.solven.holymolap.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.codahale.metrics.Meter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.RowsConsumerStatus;
import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.cube.index.IHolyCellSet;
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
	public RoaringBitmap computeNextCellRows(IHolyCellSet cellSet,
			Collection<String> axesKeys,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		if (candidateRows.isEmpty()) {
			return candidateRows;
		}

		if (axesKeys.isEmpty()) {
			// All rows match as we requested the grand total
			rowAggregateConsumer.accept(new RawCoordinatesToBitmap(candidateRows, new long[0]));

			return candidateRows;
		}

		// Pick-up next valid row
		long rowToConsider = Util.toUnsignedLong(candidateRows.select(0));

		// This array will hold the indexes of the values of the next coordinate
		// matching the selected wildcards
		final long[] valueIndexes;
		final List<RoaringBitmap> matchingRowsBitmaps;
		{
			valueIndexes = new long[axesKeys.size()];

			// The list of bitmaps matching next row siblings
			matchingRowsBitmaps = new ArrayList<>(1 + axesKeys.size());
			matchingRowsBitmaps.add(candidateRows);

			int i = -1;
			for (String wildcardKey : axesKeys) {
				i++;

				int wildcardKeyIndex = cellSet.getAxisIndex(wildcardKey);

				long valueIndex = cellSet.getCellCoordinateRef(rowToConsider, wildcardKeyIndex);

				if (valueIndex == IHolyCellSet.NOT_INDEXED) {
					throw new IllegalStateException("We are considering a row (" + rowToConsider
							+ ") which does not contribute to key: "
							+ wildcardKey);
				}

				valueIndexes[i] = valueIndex;

				RoaringBitmap valueBitmap = cellSet.getCoordinateToBitmap(wildcardKeyIndex, valueIndex);

				matchingRowsBitmaps.add(valueBitmap);
			}
		}

		RoaringBitmap matchingRowsBitmap = FastAggregation.and(matchingRowsBitmaps.iterator());

		if (!matchingRowsBitmap.contains(Ints.checkedCast(rowToConsider))) {
			throw new IllegalStateException("We should have spot at least current row");
		}

		rowAggregateConsumer.accept(new RawCoordinatesToBitmap(matchingRowsBitmap, valueIndexes));

		return matchingRowsBitmap;
	}

	public void computeParallelNextCellRows(IHolyCellSet index,
			List<String> wildcardKeys,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		computeParallelNextCellRows(index, wildcardKeys, LongLists.EMPTY_LIST, candidateRows, rowAggregateConsumer);
	}

	protected void computeParallelNextCellRows(final IHolyCellSet index,
			final List<String> wildcardKeys,
			final LongList valueIndexes,
			final RoaringBitmap candidateRows,
			final Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		AtomicLong nbAsyncTasks = new AtomicLong();

		RowsConsumerStatus status = new RowsConsumerStatus(candidateRows.getCardinality());
		activeGroupBy.add(status);

		try {
			computeParallelNextCellRows(index,
					wildcardKeys,
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

	protected void computeParallelNextCellRows(final IHolyCellSet cellSet,
			final List<String> axes,
			final LongList coordinateRefs,
			final RoaringBitmap candidateRows,
			final Consumer<RawCoordinatesToBitmap> rowAggregateConsumer,
			final Executor es,
			final AtomicLong nbAsyncTasks,
			final RowsConsumerStatus status) {
		if (axes.size() == coordinateRefs.size()) {
			// All rows match as we requested the grand total
			rowAggregateConsumer.accept(new RawCoordinatesToBitmap(candidateRows, coordinateRefs.toLongArray()));

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
					String wildcardKey = axes.get(coordinateRefs.size());

					int wildcardKeyIndex = cellSet.getAxisIndex(wildcardKey);

					final long valueIndex = cellSet.getCellCoordinateRef(rowToConsider, wildcardKeyIndex);

					if (valueIndex == IHolyCellSet.NOT_INDEXED) {
						throw new IllegalStateException("We are considering a row (" + rowToConsider
								+ ") which does not contribute to key: "
								+ wildcardKey);
					}

					final RoaringBitmap valueBitmap = cellSet.getCoordinateToBitmap(wildcardKeyIndex, valueIndex);

					final RoaringBitmap finalCandidateRowsLeft = candidateRowsLeft;
					Runnable command = new Runnable() {

						@Override
						public void run() {
							try {
								computeOnMatchingRows(finalCandidateRowsLeft,
										valueBitmap,
										rowToConsider,
										coordinateRefs,
										valueIndex,
										cellSet,
										axes,
										rowAggregateConsumer,
										es,
										nbAsyncTasks,
										status);
							} finally {
								nbAsyncTasks.decrementAndGet();
							}
						}
					};

					if (candidateRowsLeft.getCardinality() > 1024) {
						// Fork only if there is enough rows to consider

						nbAsyncTasks.incrementAndGet();
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
			IHolyCellSet index,
			List<String> wildcardKeys,
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
				wildcardKeys,
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
	public RoaringBitmap consumeNextCellRows(final IHolyCellSet index,
			final Collection<String> wildcards,
			final RoaringBitmap rows,
			Consumer<RawCoordinatesToBitmap> consumer) {
		if (rows.isEmpty()) {
			return rows;
		} else {
			RoaringBitmap matchingRows = computeNextCellRows(index, wildcards, rows, consumer);

			// LOGGER.debug("We consumed {} rows out of {}",
			// matchingRows.getCardinality(), rows.getCardinality());

			// Do not consider the matching rows for future
			// aggregation
			// TODO: could we modify rows in place?
			return RoaringBitmap.andNot(rows, matchingRows);
		}
	}

	public Iterator<RawCoordinatesToBitmap> nextCellRows(final IHolyCellSet index,
			final Collection<String> wildcards,
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
					leftRows = consumeNextCellRows(index, wildcards, leftRows, consumer);

					int nbLeftRows = leftRows.getCardinality();

					meter.mark(nbRowsBefore - nbLeftRows);
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
}
