package eu.solven.holymolap.compression.doubles;

import java.nio.DoubleBuffer;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;

public class ByteBufferDoubleList extends AbstractDoubleList {
	final DoubleBuffer doubleBuffer;

	public ByteBufferDoubleList(DoubleBuffer doubleBuffer) {
		this.doubleBuffer = doubleBuffer;
	}

	@Override
	public double getDouble(int index) {
		return doubleBuffer.get(index);
	}

	@Override
	public int size() {
		return doubleBuffer.limit();
	}

}
