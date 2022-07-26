package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.codahale.metrics.Meter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.aggregate.RawCoordinatesToBitmap;
import eu.solven.holymolap.cube.index.IHolyCubeIndex;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;

@ManagedResource
public class PositionIndexBuilder {
	protected static final Logger LOGGER = LoggerFactory.getLogger(PositionIndexBuilder.class);

	/**
	 * The number of rows of currently active index operations
	 */
	protected final AtomicLong rowsBeingActiveIndexed = new AtomicLong();

	/**
	 * The number of rows which have been already indexed in an index operation which is still active (i.e. which have
	 * more rows to index)
	 */
	protected final AtomicLong rowsDoneActiveIndexed = new AtomicLong();

	protected final Set<RowsConsumerStatus> activeGroupBy = Sets.newConcurrentHashSet();

	@ManagedAttribute
	public long getRowsBeingActiveIndexed() {
		return rowsBeingActiveIndexed.get();
	}

	@ManagedAttribute
	public long getRowsDoneActiveIndexed() {
		return rowsDoneActiveIndexed.get();
	}

	public void buildIndex(int keyIndex, IDataHolder dataHolder, LongList valuesIndex, Consumer<Integer> onKeyIndexed) {
		buildIndex(keyIndex, dataHolder, valuesIndex);

		onKeyIndexed.accept(keyIndex);
		// TODO: we should be able to tell indexed rows while indexing
		// This would enable querying even before end of indexation
		// indexRow.set(nbRows);
	}

	public void buildIndex(int keyIndex, IDataHolder dataHolder, LongList valuesIndex) {
		LOGGER.debug("Start building index for key {}", keyIndex);

		for (int valueIndex = 0; valueIndex < dataHolder.getKeyCardinality(keyIndex); valueIndex++) {
			RoaringBitmap valueIndexToBitmap = dataHolder.getValueIndexToBitmap(keyIndex, valueIndex);

			int nbRows = valueIndexToBitmap.getCardinality();

			rowsBeingActiveIndexed.addAndGet(nbRows);
			try {
				IntIterator valueRowsIterator = valueIndexToBitmap.getIntIterator();

				while (valueRowsIterator.hasNext()) {
					int nextRow = valueRowsIterator.next();
					rowsDoneActiveIndexed.incrementAndGet();

					long previousValueIndex = valuesIndex.set(nextRow, valueIndex);

					if (previousValueIndex != IHolyCubeIndex.NOT_INDEXED) {
						if (previousValueIndex != valueIndex) {
							throw new IllegalStateException("The row " + nextRow
									+ " is associated to several values for the same row: "
									+ valueIndex
									+ " and "
									+ previousValueIndex);
						} else {
							LOGGER.debug("We unexpectedly indexed again {}", nextRow);
						}
					}
				}
			} finally {
				rowsBeingActiveIndexed.addAndGet(-nbRows);
				rowsDoneActiveIndexed.addAndGet(-nbRows);
			}
		}
	}

	/**
	 * 
	 * @param index
	 * @param wildards
	 * @param candidateRows
	 * @param keyToValueIndexToBitmap
	 * @return
	 * @return the matching rows
	 */
	public RoaringBitmap computeNextCellRows(IHolyCubeIndex index,
			Collection<String> wildcardKeys,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		if (candidateRows.isEmpty()) {
			return candidateRows;
		}

		if (wildcardKeys.isEmpty()) {
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
			valueIndexes = new long[wildcardKeys.size()];

			// The list of bitmaps matching next row siblings
			matchingRowsBitmaps = new ArrayList<>(1 + wildcardKeys.size());
			matchingRowsBitmaps.add(candidateRows);

			int i = -1;
			for (String wildcardKey : wildcardKeys) {
				i++;

				int wildcardKeyIndex = index.getAxisIndex(wildcardKey);

				long valueIndex = index.getCoordinateIndex(wildcardKeyIndex, rowToConsider);

				if (valueIndex == IHolyCubeIndex.NOT_INDEXED) {
					throw new IllegalStateException("We are considering a row (" + rowToConsider
							+ ") which does not contribute to key: "
							+ wildcardKey);
				}

				valueIndexes[i] = valueIndex;

				RoaringBitmap valueBitmap = index.getValueIndexToBitmap(wildcardKeyIndex, valueIndex);

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

	public void computeParallelNextCellRows(IHolyCubeIndex index,
			List<String> wildcardKeys,
			RoaringBitmap candidateRows,
			Consumer<RawCoordinatesToBitmap> rowAggregateConsumer) {
		computeParallelNextCellRows(index, wildcardKeys, LongLists.EMPTY_LIST, candidateRows, rowAggregateConsumer);
	}

	protected void computeParallelNextCellRows(final IHolyCubeIndex index,
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

	protected void computeParallelNextCellRows(final IHolyCubeIndex index,
			final List<String> wildcardKeys,
			final LongList valueIndexes,
			final RoaringBitmap candidateRows,
			final Consumer<RawCoordinatesToBitmap> rowAggregateConsumer,
			final Executor es,
			final AtomicLong nbAsyncTasks,
			final RowsConsumerStatus status) {
		if (wildcardKeys.size() == valueIndexes.size()) {
			// All rows match as we requested the grand total
			rowAggregateConsumer.accept(new RawCoordinatesToBitmap(candidateRows, valueIndexes.toLongArray()));

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
					String wildcardKey = wildcardKeys.get(valueIndexes.size());

					int wildcardKeyIndex = index.getAxisIndex(wildcardKey);

					final long valueIndex = index.getCoordinateIndex(wildcardKeyIndex, rowToConsider);

					if (valueIndex == IHolyCubeIndex.NOT_INDEXED) {
						throw new IllegalStateException("We are considering a row (" + rowToConsider
								+ ") which does not contribute to key: "
								+ wildcardKey);
					}

					final RoaringBitmap valueBitmap = index.getValueIndexToBitmap(wildcardKeyIndex, valueIndex);

					if (candidateRowsLeft.getCardinality() > 1024) {
						// Fork only if there is enough rows to consider
						final RoaringBitmap finalCandidateRowsLeft = candidateRowsLeft;

						nbAsyncTasks.incrementAndGet();
						es.execute(new Runnable() {

							@Override
							public void run() {
								try {
									computeOnMatchingRows(finalCandidateRowsLeft,
											valueBitmap,
											rowToConsider,
											valueIndexes,
											valueIndex,
											index,
											wildcardKeys,
											rowAggregateConsumer,
											es,
											nbAsyncTasks,
											status);
								} finally {
									nbAsyncTasks.decrementAndGet();
								}
							}
						});
					} else {
						computeOnMatchingRows(candidateRowsLeft,
								valueBitmap,
								rowToConsider,
								valueIndexes,
								valueIndex,
								index,
								wildcardKeys,
								rowAggregateConsumer,
								es,
								nbAsyncTasks,
								status);
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
			IHolyCubeIndex index,
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
	public RoaringBitmap consumeNextCellRows(final IHolyCubeIndex index,
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

	public Iterator<RawCoordinatesToBitmap> nextCellRows(final IHolyCubeIndex index,
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
					if (now > start.get() + 1000) {
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
