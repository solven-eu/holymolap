package eu.solven.holymolap.sink.record;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class FastEntry implements IHolyRecord {
	final List<String> axes;

	protected final int[] objectIndexes;
	protected final Object[] objects;

	protected final int[] doubleIndexes;
	protected final double[] doubles;

	protected final int[] longIndexes;
	protected final long[] longs;

	public FastEntry(List<String> axes, Object[] objects) {
		this(axes, objects, new long[0], new double[0]);
	}

	public FastEntry(List<String> axes, Object[] objects, long[] longs, double[] doubles) {
		this.axes = ImmutableList.copyOf(axes);

		this.objects = objects;
		this.longs = longs;
		this.doubles = doubles;

		if (objects == null) {
			objectIndexes = new int[0];
		} else {
			objectIndexes = new int[objects.length];
			for (int i = 0; i < objects.length; i++) {
				objectIndexes[i] = i;
			}
		}

		if (doubles == null) {
			doubleIndexes = new int[0];
		} else {
			doubleIndexes = new int[doubles.length];
			for (int i = 0; i < doubles.length; i++) {
				doubleIndexes[i] = objectIndexes.length + i;
			}
		}

		if (longs == null) {
			longIndexes = new int[0];
		} else {
			longIndexes = new int[longs.length];
			for (int i = 0; i < longs.length; i++) {
				longIndexes[i] = objectIndexes.length + doubleIndexes.length + i;
			}
		}

		int nbAxes = this.axes.size();
		if (nbAxes != objectIndexes.length + doubleIndexes.length + longIndexes.length) {
			throw new IllegalArgumentException("Number of axes (" + nbAxes
					+ ") differs to number of coordinates (o="
					+ objectIndexes.length
					+ ", d="
					+ doubleIndexes.length
					+ ", l="
					+ longIndexes.length
					+ ")");
		}
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < objectIndexes.length; i++) {
			visitor.onObject(i, objects[i]);
		}
		int shiftForLong = objectIndexes.length;
		for (int i = 0; i < longIndexes.length; i++) {
			visitor.onObject(shiftForLong + i, longs[i]);
		}
		int shiftForDouble = shiftForLong + longIndexes.length;
		for (int i = 0; i < doubleIndexes.length; i++) {
			visitor.onObject(shiftForDouble + i, doubles[i]);
		}
	}

}
