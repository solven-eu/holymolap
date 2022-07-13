package eu.solven.holymolap;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.sink.IKeyValuesIndex;

@ManagedResource
public class RoaringCube implements IHolyCube {
	protected static final Logger LOGGER = LoggerFactory.getLogger(RoaringCube.class);

	protected final int nbRows;

	/**
	 * The bitmap tells which rows express given key
	 */
	protected final List<? extends RoaringBitmap> keyToBitmap;

	/**
	 * For each key, gives the value as double, which would be valid only where
	 * the key-bitmap is true
	 */
	protected final List<DoubleList> keyToDoubles;

	protected final IRoaringCubeIndex index;

	public RoaringCube(int nbRows, List<?> keyIndexToKey, List<RoaringBitmap> keyIndexToBitmap, List<IKeyValuesIndex> keyIndexToValueIndex,
			List<? extends List<RoaringBitmap>> keyIndexToValueIndexToBitmap, List<DoubleList> keyIndexToDoubles, List<IntList> keyIndexToInts) {
		this.nbRows = nbRows;
		// this.keyIndexToKey = keyIndexToKey;
		this.keyToBitmap = keyIndexToBitmap;
		// this.keyToValueToBitmap = keyIndexToValueIndexToBitmap;
		this.keyToDoubles = keyIndexToDoubles;
		// this.keyToInts = keyIndexToInts;

		this.index = new RoaringCubeIndex(nbRows, keyIndexToKey, keyIndexToValueIndex, new DataHolder(nbRows, keyIndexToValueIndexToBitmap,
				keyIndexToInts, keyToBitmap, keyIndexToValueIndex));
	}

	@Override
	public String toString() {
		return "#Rows: " + nbRows + ", Keys=" + index.keySet();
	}

	/**
	 * Make an initially empty cube
	 */
	public RoaringCube() {
		this(0, Collections.emptyList(), Collections.<RoaringBitmap> emptyList(), Collections.<IKeyValuesIndex> emptyList(), Collections
				.<List<RoaringBitmap>> emptyList(), Collections.<DoubleList> emptyList(), Collections.<IntList> emptyList());
	}

	@Override
	public Collection<? extends RoaringBitmap> getKeyBitmaps(Collection<?> keys) {
		List<RoaringBitmap> bitmaps = new ArrayList<>();

		for (Object wildcardKey : keys) {
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
	public Collection<RoaringBitmap> getValueBitmaps(Map<?, ?> filter) {
		List<RoaringBitmap> bitmaps = new ArrayList<>();

		for (Entry<?, ?> entry : filter.entrySet()) {
			if (entry.getValue() == null) {
				throw new IllegalArgumentException("One can not query a null value");
			} else {
				RoaringBitmap valueBitmap = index.getBitmap(entry.getKey(), entry.getValue());

				if (valueBitmap == null) {
					// This key can not be filtered: the view is empty
					return Collections.singleton(new RoaringBitmap());
				} else if (entry.getValue() instanceof Number) {
					throw new UnsupportedOperationException("TODO");
				} else {
					// RoaringBitmap valueBitmap =
					// valueToBitmap.get(index.getValueIndexToBitmap(wildcardKey)
					// entry.getValue());

					// if (valueBitmap == null) {
					// // There is no row for this value: the view is empty
					// return Collections.singleton(new RoaringBitmap());
					// } else {
					bitmaps.add(valueBitmap);
					// }
				}
			}
		}

		return bitmaps;

	}

	@Override
	public NavigableMap<?, ?> convertToCoordinates(int row, Set<?> keys) {
		NavigableMap<Object, Object> coordinates = new ConcurrentSkipListMap<>();

		for (Object key : keys) {
			coordinates.put(key, index.getValueAtRow(key, row));
		}

		return coordinates;
	}

	@Override
	public Object convertKeyIndexToKey(int keyIndex) {
		return index.getKeyAtIndex(keyIndex);
	}

	@Override
	public int getNbRows() {
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
