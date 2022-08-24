package eu.solven.holymolap.sink.record;

public class FastEntry implements IHolyRecord {
	protected final int[] objectIndexes;
	protected final Object[] objects;

	protected final int[] doubleIndexes;
	protected final double[] doubles;

	protected final int[] intIndexes;
	protected final int[] ints;

	public FastEntry(Object[] objects) {
		this(objects, new double[0], new int[0]);
	}

	public FastEntry(Object[] objects, double[] doubles, int[] ints) {
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

}
