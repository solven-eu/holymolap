package eu.solven.holymolap.sink.record;

public interface IHolyRecordVisitor {
	void onObject(int axisIndex, Object o);

	default void onInt(int axisIndex, int i) {
		onObject(axisIndex, i);
	};

	default void onDouble(int axisIndex, double d) {
		onObject(axisIndex, d);
	};
}
