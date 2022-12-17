package eu.solven.holymolap.mutable.cellset;

import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionary;
import eu.solven.holymolap.primitives.IntArrayListFastHashCode;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * The simplest {@link IBijectiveHolyCellToRow} implementation, covering any case which would not be covered by
 * alternative implementations (e.g. due to constraints related to compression)
 * 
 * @author Benoit Lacelle
 *
 */
public class Object2IntHolyCellToRow implements IBijectiveHolyCellToRow {
	final Object2IntMap<IntList> underlying;

	public Object2IntHolyCellToRow(Object2IntMap<IntList> underlying) {
		this.underlying = underlying;

		if (underlying.defaultReturnValue() != IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			throw new IllegalArgumentException("Invalid defaultReturnValue: " + underlying.defaultReturnValue());
		}
	}

	public Object2IntHolyCellToRow() {
		this(defaultUnderlying());
	}

	protected static Object2IntMap<IntList> defaultUnderlying() {
		Object2IntOpenHashMap<IntList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	/**
	 * 
	 * @param coordinates
	 * @param willbeStored
	 *            if true, it indicates given array will be referred in the underlying for long-time storage
	 * @return an {@link IntList}, optionally compressed, but requiring to be invertible into the original IntList (as
	 *         it is used as key for rows)
	 */
	protected IntList compress(IntList coordinates, boolean willbeStored) {
		return new IntArrayListFastHashCode(coordinates);
	}

	@Override
	public int getRow(IntList coordinates) {
		return underlying.getInt(compress(coordinates, false));
	}

	@Override
	public int getMayAppendRow(IntList coordinates) {
		int newIndex = underlying.size();

		int previousValue = underlying.putIfAbsent(compress(coordinates, true), newIndex);

		if (previousValue == IMutableAxisSmallDictionary.NO_COORDINATE_INDEX) {
			// We mapped a value
			return -newIndex - 1;
		} else {
			// It was already mapped
			return previousValue;
		}
	}

	@Override
	public int size() {
		return underlying.size();
	}

}
