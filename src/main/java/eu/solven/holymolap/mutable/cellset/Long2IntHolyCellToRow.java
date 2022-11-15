package eu.solven.holymolap.mutable.cellset;

import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionary;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

/**
 * This implementation of {@link IHolyCellToRow} will compress {@link IntList} into a long by packing coordinates into a
 * long. At some point, the underlying algorithm will not be able to accept additional {@link IntList} (as not packable
 * into a long). It would then be time to switch to a different algorithm.
 * 
 * This implementation trades CPU to gain RAM.
 * 
 * @author Benoit Lacelle
 *
 */
public class Long2IntHolyCellToRow implements IHolyCellToRow {
	final Long2IntMap underlying;

	public Long2IntHolyCellToRow(Long2IntMap underlying) {
		this.underlying = underlying;

		if (underlying.defaultReturnValue() != IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + underlying.defaultReturnValue());
		}
	}

	public Long2IntHolyCellToRow() {
		this(defaultUnderlying());
	}

	private static Long2IntMap defaultUnderlying() {
		Long2IntOpenHashMap cellToRow = new Long2IntOpenHashMap();

		cellToRow.defaultReturnValue(IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	@Override
	public int getRow(IntList coordinates) {
		long coordinatesAsLong = packToLong(coordinates);
		return underlying.get(coordinatesAsLong);
	}

	@Override
	public int registerRow(IntList coordinates) {
		int newIndex = underlying.size();

		long coordinatesAsLong = packToLong(coordinates);
		underlying.put(coordinatesAsLong, newIndex);

		return newIndex;
	}

	@Override
	public int size() {
		return underlying.size();
	}

	private long packToLong(IntList coordinates) {
		return FibonacciEncoding.fibonacciEncodingToLong(coordinates);
	}

}
