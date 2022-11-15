package eu.solven.holymolap.mutable.cellset;

import eu.solven.holymolap.mutable.axis.IMutableAxisSmallDictionary;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class Object2IntHolyCellToRow implements IHolyCellToRow {
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

	private static Object2IntMap<IntList> defaultUnderlying() {
		Object2IntOpenHashMap<IntList> cellToRow = new Object2IntOpenHashMap<>();

		cellToRow.defaultReturnValue(IMutableAxisSmallDictionary.NO_COORDINATE_INDEX);

		return cellToRow;
	}

	@Override
	public int getRow(IntList coordinates) {
		return underlying.getInt(coordinates);
	}

	@Override
	public int registerRow(IntList coordinates) {
		int newIndex = underlying.size();

		underlying.put(coordinates, newIndex);

		return newIndex;
	}

	@Override
	public int size() {
		return underlying.size();
	}

}
