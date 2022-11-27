package eu.solven.holymolap.mutable.axis;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

// This may be compressed as we just append into it
public class AppendOnlyIntArrayList implements IAppendOnlyIntList {
	final IntList intList;

	public AppendOnlyIntArrayList(IntList intList) {
		this.intList = intList;
	}

	public AppendOnlyIntArrayList() {
		this(new IntArrayList());
	}

	@Override
	public void append(int coordinateRef) {
		// TODO In fact, this would fail before Integer.MAX_VALUE as JVM restrict arrays to MAX_VALUE-N
		if (intList.size() == Integer.MAX_VALUE) {
			throw new IllegalStateException("This structure is full");
		}

		intList.add(coordinateRef);
	}

	@Override
	public long size() {
		return intList.size();
	}

	@Override
	public void getElements(int from, int[] a, int offset, int length) {
		intList.getElements(from, a, offset, length);
	}

}
