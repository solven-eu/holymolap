package eu.solven.holymolap.sink.record;

import java.util.List;

public interface IHolyRecord {

	/**
	 * 
	 * @return the {@link List} of axes which may be returned by this record
	 */
	List<String> getAxes();

	void accept(IHolyRecordVisitor visitor);

}
