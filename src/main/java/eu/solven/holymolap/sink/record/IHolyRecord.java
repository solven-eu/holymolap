package eu.solven.holymolap.sink.record;

public interface IHolyRecord {
	int[] objectIndexes();

	Object getObject(int axisIndex);

	int[] intIndexes();

	int getInt(int axisIndex);

	int[] doubleIndexes();

	double getDouble(int axisIndex);
}
