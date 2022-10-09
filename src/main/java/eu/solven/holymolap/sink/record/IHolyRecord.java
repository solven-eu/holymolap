package eu.solven.holymolap.sink.record;

import java.util.List;

public interface IHolyRecord {

	List<String> getAxes();

	void accept(IHolyRecordVisitor visitor);
}
