package eu.solven.holymolap.sink.record;

import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public interface IHolyMeasuresRecord {

	/**
	 * 
	 * @return the {@link List} of axes which may be returned by this record
	 */
	List<IMeasuredAxis> getMeasuredAxes();

	void accept(IHolyRecordVisitor visitor);

}
