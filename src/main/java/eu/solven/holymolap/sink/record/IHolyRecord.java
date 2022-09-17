package eu.solven.holymolap.sink.record;

import java.util.List;

public interface IHolyRecord {

	List<String> getAxes();

	int[] objectIndexes();

	Object getObject(int axisIndex);

	int[] intIndexes();

	int getInt(int axisIndex);

	int[] doubleIndexes();

	double getDouble(int axisIndex);

	void accept(IHolyRecordVisitor visitor);
}
