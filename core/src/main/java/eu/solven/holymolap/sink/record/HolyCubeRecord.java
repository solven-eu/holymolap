package eu.solven.holymolap.sink.record;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

public class HolyCubeRecord implements IHolyCubeRecord {
	final IHolyRecord cellsetRecord;
	final IHolyMeasuresRecord measuresRecord;

	public HolyCubeRecord(IHolyRecord cellsetRecord, IHolyMeasuresRecord measuresRecord) {
		this.cellsetRecord = cellsetRecord;
		this.measuresRecord = measuresRecord;
	}

	@Override
	public IHolyRecord getCellsetRecord() {
		return cellsetRecord;
	}

	@Override
	public IHolyMeasuresRecord getMeasuresTableRecord() {
		return measuresRecord;
	}

	@Override
	public String toString() {
		ToStringHelper toStringhelper = MoreObjects.toStringHelper(this);

		toStringhelper.add("cell", cellsetRecord.toString());
		toStringhelper.add("measures", measuresRecord.toString());

		return toStringhelper.toString();
	}
}
