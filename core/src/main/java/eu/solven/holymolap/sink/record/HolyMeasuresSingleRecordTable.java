package eu.solven.holymolap.sink.record;

import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class HolyMeasuresSingleRecordTable implements IHolyMeasuresRecordsTable {
	protected final IHolyMeasuresRecord measuresRecord;

	public HolyMeasuresSingleRecordTable(IHolyMeasuresRecord measuresRecord) {
		this.measuresRecord = measuresRecord;
	}

	@Override
	public List<IMeasuredAxis> getMeasures() {
		return measuresRecord.getMeasuredAxes();
	}

	@Override
	public void accept(IHolyRecordsTableVisitor visitor) {
		measuresRecord.accept((axisIndex, o) -> {
			visitor.onObject(axisIndex, Collections.singletonList(o));
		});
	}

	@Override
	public long size() {
		return 1;
	}

}
