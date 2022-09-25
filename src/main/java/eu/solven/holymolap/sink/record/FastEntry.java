package eu.solven.holymolap.sink.record;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class FastEntry implements IHolyRecord {
	final List<String> axes;

	protected final int[] objectIndexes;
	protected final Object[] objects;

	protected final int[] doubleIndexes;
	protected final double[] doubles;

	protected final int[] intIndexes;
	protected final int[] ints;

	public FastEntry(List<String> axes, Object[] objects) {
		this(axes, objects, new double[0], new int[0]);
	}

	public FastEntry(List<String> axes, Object[] objects, double[] doubles, int[] ints) {
		this.axes = ImmutableList.copyOf(axes);

		this.objects = objects;
		this.doubles = doubles;
		this.ints = ints;

		if (ints == null) {
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

		if (ints == null) {
			intIndexes = new int[0];
		} else {
			intIndexes = new int[ints.length];
			for (int i = 0; i < ints.length; i++) {
				intIndexes[i] = objectIndexes.length + doubleIndexes.length + i;
			}
		}

		int nbAxes = this.axes.size();
		if (nbAxes != objectIndexes.length + doubleIndexes.length + intIndexes.length) {
			throw new IllegalArgumentException("Number of axes (" + nbAxes
					+ ") differs to number of coordinates (o="
					+ objectIndexes.length
					+ ", d="
					+ doubleIndexes.length
					+ ", i="
					+ intIndexes.length
					+ ")");
		}
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public int[] objectIndexes() {
		return objectIndexes;
	}

	@Override
	public Comparable<?> getObject(int keyIndex) {
		return (Comparable<?>) objects[keyIndex];
	}

	@Override
	public int[] doubleIndexes() {
		return doubleIndexes;
	}

	@Override
	public double getDouble(int keyIndex) {
		return doubles[keyIndex - objectIndexes.length];
	}

	@Override
	public int[] intIndexes() {
		return intIndexes;
	}

	@Override
	public int getInt(int keyIndex) {
		return ints[keyIndex - objectIndexes.length - doubleIndexes.length];
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < objectIndexes.length; i++) {
			visitor.onObject(i, objects[i]);
		}
		for (int i = 0; i < intIndexes.length; i++) {
			visitor.onObject(i, ints[i]);
		}
		for (int i = 0; i < doubleIndexes.length; i++) {
			visitor.onObject(i, doubles[i]);
		}
	}

}
