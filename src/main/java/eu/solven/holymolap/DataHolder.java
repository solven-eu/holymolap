package eu.solven.holymolap;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;

import com.google.common.primitives.Ints;

import eu.solven.holymolap.sink.IKeyValuesIndex;
import eu.solven.holymolap.sink.KeyValuesIndex;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class DataHolder implements IDataHolder {

	protected final int nbRow;
	protected final List<List<? extends RoaringBitmap>> keyIndexToValueIndexToBitmap;
	protected final List<? extends IntList> keyIndexToInts;
	protected final List<? extends RoaringBitmap> keyToBitmap;
	protected final List<? extends IKeyValuesIndex> keyIndexToValueIndex;

	// protected final List<IntList> keyIndexToValueIndexToFirstRow;

	public DataHolder(int nbRow,
			List<? extends List<? extends RoaringBitmap>> keyIndexToValueIndexToBitmap,
			List<? extends IntList> keyIndexToInts,
			List<? extends RoaringBitmap> keyToBitmap,
			List<? extends IKeyValuesIndex> keyIndexToValueIndex) {
		this.nbRow = nbRow;
		this.keyIndexToValueIndexToBitmap = new ArrayList<>(keyIndexToValueIndexToBitmap);
		this.keyIndexToInts = keyIndexToInts;

		this.keyToBitmap = keyToBitmap;
		this.keyIndexToValueIndex = keyIndexToValueIndex;

		// keyIndexToIntCardinality = new
		// Int2IntOpenHashMap(keyIndexToInts.size());
		// keyIndexToIntCardinality.defaultReturnValue(IRoaringCubeIndex.NOT_INDEXED);
		// keyIndexToValueIndexToFirstRow = Arrays.asList(new
		// IntList[keyIndexToInts.size()]);
	}

	@Override
	public long getKeyCardinality(int keyIndex) {
		List<? extends RoaringBitmap> valueIndexToBitmap = keyIndexToValueIndexToBitmap.get(keyIndex);

		if (valueIndexToBitmap == null) {
			IntList rowIndexToInt = keyIndexToInts.get(keyIndex);

			if (rowIndexToInt == null) {
				throw new RuntimeException("We can not index the keyIndex: " + keyIndex);
			} else {
				synchronized (keyIndexToValueIndexToBitmap) {
					valueIndexToBitmap = keyIndexToValueIndexToBitmap.get(keyIndex);
					if (valueIndexToBitmap == null) {
						List<RoaringBitmap> newValueIndexToBitmap = new ArrayList<>();
						valueIndexToBitmap = newValueIndexToBitmap;

						IKeyValuesIndex keyValuesIndex = new KeyValuesIndex();
						// IntSet consideredValues = new IntOpenHashSet();
						for (int rowWithValue : keyToBitmap.get(keyIndex)) {
							int currentValue = rowIndexToInt.getInt(rowWithValue);

							long currentValueIndex = keyValuesIndex.getValueIndex(currentValue);
							if (currentValueIndex == IKeyValuesIndex.NOT_INDEXED) {
								// This is the first encounter of this value
								currentValueIndex = keyValuesIndex.mapValueIndex(currentValue);

								assert currentValueIndex == valueIndexToBitmap.size();
								newValueIndexToBitmap.add(new RoaringBitmap());
							}

							newValueIndexToBitmap.get(Ints.checkedCast(currentValueIndex)).add(rowWithValue);
						}

						keyIndexToValueIndexToBitmap.set(keyIndex, valueIndexToBitmap);
					}
				}
			}
		}

		return valueIndexToBitmap.size();
	}

	@Override
	public RoaringBitmap getValueIndexToBitmap(int keyIndex, long valueIndex) {
		List<? extends RoaringBitmap> valueIndexToBitmap = keyIndexToValueIndexToBitmap.get(keyIndex);

		if (valueIndexToBitmap == null) {
			IntList rowIndexToInt = keyIndexToInts.get(keyIndex);

			if (rowIndexToInt == null) {
				throw new RuntimeException("We can not index the keyIndex: " + keyIndex);
			} else {
				if (rowIndexToInt instanceof IntArrayList) {
					return RoaringBitmap.bitmapOf(((IntArrayList) rowIndexToInt).elements());
				} else {
					return RoaringBitmap.bitmapOf(rowIndexToInt.toIntArray());
				}
			}
		} else {
			return valueIndexToBitmap.get(Ints.checkedCast(valueIndex));
		}
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		for (List<? extends RoaringBitmap> bitmapList : keyIndexToValueIndexToBitmap) {
			for (RoaringBitmap bitmap : bitmapList) {
				sizeInBytes += bitmap.getSizeInBytes();
			}
		}

		return sizeInBytes;
	}
}
