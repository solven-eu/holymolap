package eu.solven.holymolap.sink.record;

public interface IHolyRecordVisitor {
	void onObject(int axisIndex, Object o);

	default void onLong(int axisIndex, long l) {
		onObject(axisIndex, l);
	};

	default void onDouble(int axisIndex, double d) {
		onObject(axisIndex, d);
	};
}
