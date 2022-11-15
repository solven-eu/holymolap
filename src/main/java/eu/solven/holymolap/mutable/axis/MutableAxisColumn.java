package eu.solven.holymolap.mutable.axis;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * A column of coordinates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://stackoverflow.com/questions/32917973/thread-safe-list-that-only-needs-to-support-random-access-and-appends
public class MutableAxisColumn implements IMutableAxisSmallColumn {
	final ILazyMutableAxisSmallDictionary coordinateToIndex;
	final IntList rowToCoordinate;

	final AtomicBoolean isLocked = new AtomicBoolean();

	protected MutableAxisColumn(ILazyMutableAxisSmallDictionary coordinateToIndex, IntList rowToCoordinate) {
		this.coordinateToIndex = coordinateToIndex;
		this.rowToCoordinate = rowToCoordinate;
	}

	public MutableAxisColumn() {
		this.coordinateToIndex = new LazyTypeAxisDictionary();

		this.rowToCoordinate = new IntArrayList();
	}

	@Override
	public synchronized void appendCoordinate(Object coordinate) {
		if (rowToCoordinate.size() == Integer.MAX_VALUE) {
			throw new IllegalStateException("This structure is full");
		}

		int nextRowCoordinate = coordinateToIndex.asObjects().getIndexMayAppend(coordinate);

		appendCoordinateRef(nextRowCoordinate);
	}

	@Override
	public synchronized void appendCoordinateRef(int coordinateIndex) {
		rowToCoordinate.add(coordinateIndex);
	}

	@Override
	public synchronized long getRows() {
		return rowToCoordinate.size();
	}

	@Override
	public ILazyMutableAxisSmallDictionary getCoordinateToRef() {
		isLocked.set(true);
		return coordinateToIndex;
	}

	@Override
	public void getRowToIndex(int from, int a[], int offset, int length) {
		// We are reading the data: make this read-only
		isLocked.set(true);

		rowToCoordinate.getElements(from, a, offset, length);
	}
}
