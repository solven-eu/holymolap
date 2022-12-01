package eu.solven.holymolap.sink.record;

import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public interface IHolyMeasuresRecordsTable {

	/**
	 * 
	 * @return the {@link List} of axes which may be returned by records in this table
	 */
	List<IMeasuredAxis> getMeasures();

	void accept(IHolyRecordsTableVisitor visitor);

	long size();

}
