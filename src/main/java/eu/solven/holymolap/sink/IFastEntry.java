package eu.solven.holymolap.sink;

public interface IFastEntry {
	int[] expressedObjectIndexes();

	Comparable<?> getValue(int keyIndex);

	int[] expressedIntIndexes();

	int getInt(int keyIndex);

	int[] expressedDoubleIndexes();

	double getDouble(int keyIndex);
}
