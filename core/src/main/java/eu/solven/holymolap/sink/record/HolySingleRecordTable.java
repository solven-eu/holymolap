package eu.solven.holymolap.sink.record;

import java.util.Collections;
import java.util.List;

public class HolySingleRecordTable implements IHolyRecordsTable {
	protected final IHolyRecord cubeRecord;

	public HolySingleRecordTable(IHolyRecord cubeRecord) {
		super();
		this.cubeRecord = cubeRecord;
	}

	@Override
	public long size() {
		return 1;
	}

	@Override
	public List<String> getAxes() {
		return cubeRecord.getAxes();
	}

	@Override
	public void accept(IHolyRecordsTableVisitor visitor) {
		cubeRecord.accept((axisIndex, o) -> {
			visitor.onObject(axisIndex, Collections.singletonList(o));
		});
	}

}
