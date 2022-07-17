package eu.solven.holymolap.cube;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.DataHolder;
import eu.solven.holymolap.IRoaringCubeIndex;
import eu.solven.holymolap.RoaringCubeIndex;
import eu.solven.holymolap.sink.IKeyValuesIndex;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasColumns;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAnd;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAxisEquals;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterOr;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntList;

@ManagedResource
public class HolyCube implements IHolyCube {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCube.class);

	protected final int nbRows;

	/**
	 * The bitmap tells which rows express given key
	 */
	protected final List<? extends RoaringBitmap> keyToBitmap;

	/**
	 * For each key, gives the value as double, which would be valid only where the key-bitmap is true
	 */
	protected final List<DoubleList> keyToDoubles;

	protected final IRoaringCubeIndex index;

	public HolyCube(int nbRows,
			List<String> keyIndexToKey,
			List<RoaringBitmap> keyIndexToBitmap,
			List<IKeyValuesIndex> keyIndexToValueIndex,
			List<? extends List<RoaringBitmap>> keyIndexToValueIndexToBitmap,
			List<DoubleList> keyIndexToDoubles,
			List<IntList> keyIndexToInts) {
		this.nbRows = nbRows;
		// this.keyIndexToKey = keyIndexToKey;
		this.keyToBitmap = keyIndexToBitmap;
		// this.keyToValueToBitmap = keyIndexToValueIndexToBitmap;
		this.keyToDoubles = keyIndexToDoubles;
		// this.keyToInts = keyIndexToInts;

		this.index = new RoaringCubeIndex(nbRows,
				keyIndexToKey,
				keyIndexToValueIndex,
				new DataHolder(nbRows,
						keyIndexToValueIndexToBitmap,
						keyIndexToInts,
						keyToBitmap,
						keyIndexToValueIndex));
	}

	@Override
	public String toString() {
		return "#Rows: " + nbRows + ", Keys=" + index.keySet();
	}

	/**
	 * Make an initially empty cube
	 */
	public HolyCube() {
		this(0,
				Collections.emptyList(),
				Collections.<RoaringBitmap>emptyList(),
				Collections.<IKeyValuesIndex>emptyList(),
				Collections.<List<RoaringBitmap>>emptyList(),
				Collections.<DoubleList>emptyList(),
				Collections.<IntList>emptyList());
	}

	@Override
	public Collection<? extends RoaringBitmap> getAxesBitmaps(IHasColumns hasColumns) {
		List<RoaringBitmap> bitmaps = new ArrayList<>();

		for (String wildcardKey : hasColumns.getColumns()) {
			int keyIndex = index.getKeyIndex(wildcardKey);

			if (keyIndex < 0) {
				bitmaps.add(IRoaringCubeIndex.EMPTY_BITMAP);
			} else {
				RoaringBitmap bitmap = keyToBitmap.get(keyIndex);

				if (bitmap == null) {
					LOGGER.debug("Requesting to aggregate a not-existing key: {}", wildcardKey);
					// This key can not be wildcarded: the view is empty
					bitmaps.add(IRoaringCubeIndex.EMPTY_BITMAP);
				} else {
					bitmaps.add(bitmap);
				}
			}
		}

		return bitmaps;
	}

	@Override
	public RoaringBitmap getFiltersBitmap(IHasFilters hasFilters) {
		IAxesFilter axesFilter = hasFilters.getFilters();

		long currentNbRows = getNbRows();

		RoaringBitmap all = RoaringBitmap.bitmapOfRange(0, currentNbRows);
		if (axesFilter.isMatchAll()) {
			if (axesFilter.isExclusion()) {
				// Empty
				return new RoaringBitmap();
			} else {
				return all;
			}
		} else if (axesFilter.isAxisEquals()) {
			IAxesFilterAxisEquals equalsFilter = (IAxesFilterAxisEquals) axesFilter;

			RoaringBitmap valueBitmap = index.getBitmap(equalsFilter.getAxis(), equalsFilter.getFiltered());

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, valueBitmap);
			} else {
				return valueBitmap;
			}
		} else if (axesFilter.isAnd()) {
			IAxesFilterAnd andFilter = (IAxesFilterAnd) axesFilter;

			List<RoaringBitmap> andBitmaps = new ArrayList<>();
			for (Map.Entry<String, IAxesFilter> entry : andFilter.getAnd().entrySet()) {
				RoaringBitmap entryBitmap = getFiltersBitmap(() -> entry.getValue());

				andBitmaps.add(entryBitmap);
			}

			if (andBitmaps.isEmpty()) {
				throw new IllegalStateException("Should have been caught by .matchAll");
			}

			RoaringBitmap andBitmap = RoaringBitmap.and(andBitmaps.iterator(), 0L, currentNbRows);

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, andBitmap);
			} else {
				return andBitmap;
			}
		} else if (axesFilter.isOr()) {
			IAxesFilterOr andFilter = (IAxesFilterOr) axesFilter;

			List<RoaringBitmap> orBitmaps = new ArrayList<>();
			for (IAxesFilter orFilter : andFilter.getOr()) {
				RoaringBitmap entryBitmap = getFiltersBitmap(() -> orFilter);

				orBitmaps.add(entryBitmap);
			}

			RoaringBitmap orBitmap = RoaringBitmap.or(orBitmaps.iterator(), 0L, currentNbRows);

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, orBitmap);
			} else {
				return orBitmap;
			}
		} else {
			throw new UnsupportedOperationException("filter: " + axesFilter);
		}

	}

	// @Override
	// public NavigableMap<?, ?> convertToCoordinates(int row, Set<?> keys) {
	// NavigableMap<Object, Object> coordinates = new ConcurrentSkipListMap<>();
	//
	// for (Object key : keys) {
	// coordinates.put(key, index.getValueAtRow(key, row));
	// }
	//
	// return coordinates;
	// }

	@Override
	public String indexToColumn(int keyIndex) {
		return index.getKeyAtIndex(keyIndex);
	}

	@Override
	public long getNbRows() {
		return nbRows;
	}

	// @Override
	// public RoaringBitmap getAllRows() {
	// RoaringBitmap allRows = new RoaringBitmap();
	// allRows.add(0, nbRows);
	// return allRows;
	// }

	// @Override
	// public List<?> getValuesForKey(String key) {
	// return index.getValuesForKey(key);
	// }

	@ManagedAttribute
	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (RoaringBitmap bitmap : keyToBitmap) {
			sizeInBytes += bitmap.getSizeInBytes();
		}

		for (DoubleList primitives : keyToDoubles) {
			if (primitives instanceof DoubleArrayList) {
				sizeInBytes += 8 * ((DoubleArrayList) primitives).elements().length;
			} else {
				sizeInBytes += 8 * primitives.size();
			}
		}

		return sizeInBytes;
	}

	@Override
	public IRoaringCubeIndex getIndex() {
		return index;
	}

	@Override
	public DoubleIterator readDouble(final IntIterator it, final int keyIndex, final double defaultValue) {
		return new AbstractDoubleIterator() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public double nextDouble() {
				int row = it.next();

				DoubleList doubles = keyToDoubles.get(keyIndex);

				if (doubles == null) {
					return defaultValue;
				} else {
					return doubles.getDouble(row);
				}

			}
		};
	}
}
