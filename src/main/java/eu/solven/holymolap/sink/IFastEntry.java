package eu.solven.holymolap.sink;

public interface IFastEntry {
	int[] objectAxesIndexes();

	Object getObject(int axisIndex);

	int[] intAxesIndexes();

	int getInt(int axisIndex);

	int[] doubleAxesIndexes();

	double getDouble(int axisIndex);
}
