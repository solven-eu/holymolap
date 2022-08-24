package eu.solven.holymolap.sink.record;

public class EmptyHolyRecord implements IHolyRecord {

	@Override
	public int[] objectIndexes() {
		return new int[0];
	}

	@Override
	public Object getObject(int axisIndex) {
		throw new UnsupportedOperationException("Empty");
	}

	@Override
	public int[] intIndexes() {
		return new int[0];
	}

	@Override
	public int getInt(int axisIndex) {
		throw new UnsupportedOperationException("Empty");
	}

	@Override
	public int[] doubleIndexes() {
		return new int[0];
	}

	@Override
	public double getDouble(int axisIndex) {
		throw new UnsupportedOperationException("Empty");
	}

}
