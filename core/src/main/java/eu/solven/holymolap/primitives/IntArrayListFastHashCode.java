package eu.solven.holymolap.primitives;

import java.io.Serializable;

import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;

public class IntArrayListFastHashCode extends AbstractIntList implements Serializable {
	private static final long serialVersionUID = -2813918911304076261L;

	// This will get slower with cubes with more than 128 axes
	private final static FixedLengthHashCode HASHCODE_COMPUTER = new FixedLengthHashCode(128);

	protected final IntList ints;

	public IntArrayListFastHashCode(IntList ints) {
		this.ints = ints;
	}

	@Override
	public int hashCode() {
		return HASHCODE_COMPUTER.hashCode(ints);
	}

	@Override
	public int getInt(int index) {
		return ints.getInt(index);
	}

	@Override
	public int size() {
		return ints.size();
	}
}
