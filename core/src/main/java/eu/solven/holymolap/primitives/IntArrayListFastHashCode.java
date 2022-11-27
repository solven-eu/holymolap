package eu.solven.holymolap.primitives;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class IntArrayListFastHashCode extends IntArrayList {
	private static final long serialVersionUID = -2813918911304076261L;

	// This will get slower with cubes with more than 128 axes
	private final static FixedLengthHashCode HASHCODE_COMPUTER = new FixedLengthHashCode(128);

	public IntArrayListFastHashCode(IntList ints) {
		super(ints);
	}

	@Override
	public int hashCode() {
		return HASHCODE_COMPUTER.hashCode(a);
	}
}
