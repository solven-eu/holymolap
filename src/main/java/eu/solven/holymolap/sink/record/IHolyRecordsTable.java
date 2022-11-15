package eu.solven.holymolap.sink.record;

import java.util.List;

public interface IHolyRecordsTable {

	/**
	 * 
	 * @return the {@link List} of axes which may be returned by records in this table
	 */
	List<String> getAxes();

	void accept(IHolyRecordsTableVisitor visitor);

	long size();

}
