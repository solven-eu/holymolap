package eu.solven.holymolap.indexation;

import java.util.concurrent.atomic.AtomicLong;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.immutable.table.IHolyDictionarizedTable;
import it.unimi.dsi.fastutil.longs.LongList;

@Deprecated
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

	@ManagedAttribute
	public long getRowsBeingActiveIndexed() {
		return rowsBeingActiveIndexed.get();
	}

	@ManagedAttribute
	public long getRowsDoneActiveIndexed() {
		return rowsDoneActiveIndexed.get();
	}

	public ListenableFuture<?> buildIndex(int axisIndex,
			IHasAxesWithCoordinates axesWithCoordinates,
			IHolyDictionarizedTable dictionarizedTable,
			LongList cellIndexToCoordinateRef,
			Runnable onAxisIndexed) {
		buildIndex(axisIndex, axesWithCoordinates, dictionarizedTable, cellIndexToCoordinateRef);

		onAxisIndexed.run();
		// TODO: we should be able to tell indexed rows while indexing
		// This would enable querying even before end of indexation
		// indexRow.set(nbRows);

		return Futures.immediateVoidFuture();
	}

	/**
	 * This will write into cellIndexToCoordinateRef the coordinateRef for each cellRow
	 * 
	 * @param axisIndex
	 * @param dataHolder
	 * @param cellRowToCoordinateRef
	 */
	public void buildIndex(int axisIndex,
			IHasAxesWithCoordinates axesWithCoordinates,
			IHolyDictionarizedTable dataHolder,
			LongList cellRowToCoordinateRef) {
		LOGGER.debug("Start building index for axis {}", axisIndex);

		for (int coordinateRef = 0; coordinateRef < axesWithCoordinates.getCardinality(axisIndex); coordinateRef++) {
			RoaringBitmap cellIndexes = dataHolder.getCoordinateToRows(axisIndex, coordinateRef);

			int nbRows = cellIndexes.getCardinality();

			// Check the input index is big enough
			assert cellRowToCoordinateRef.size() >= nbRows;

			rowsBeingActiveIndexed.addAndGet(nbRows);
			try {
				IntIterator cellIndexesIterator = cellIndexes.getIntIterator();

				while (cellIndexesIterator.hasNext()) {
					int nextCellIndex = cellIndexesIterator.next();
					rowsDoneActiveIndexed.incrementAndGet();

					long previousValueIndex = cellRowToCoordinateRef.set(nextCellIndex, coordinateRef);

					if (previousValueIndex != IHasAxesWithCoordinates.NOT_INDEXED) {
						if (previousValueIndex != coordinateRef) {
							throw new IllegalStateException("The row " + nextCellIndex
									+ " is associated to several values for the same row: "
									+ coordinateRef
									+ " and "
									+ previousValueIndex);
						} else {
							// TODO Track this as this would hint a performance bug
							LOGGER.debug("We unexpectedly indexed again {}", nextCellIndex);
						}
					}
				}
			} finally {
				rowsBeingActiveIndexed.addAndGet(-nbRows);
				rowsDoneActiveIndexed.addAndGet(-nbRows);
			}
		}
	}

}
