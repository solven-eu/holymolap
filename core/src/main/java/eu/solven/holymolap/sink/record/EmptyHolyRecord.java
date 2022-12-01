package eu.solven.holymolap.sink.record;

import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class EmptyHolyRecord implements IHolyRecord, IHolyMeasuresRecord {

	public static EmptyHolyRecord INSTANCE = new EmptyHolyRecord();

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public List<IMeasuredAxis> getMeasuredAxes() {
		return Collections.emptyList();
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		// nothing to visit as empty
	}

}
