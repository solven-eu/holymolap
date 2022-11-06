package eu.solven.holymolap.sink.record;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

public class HolyCubeRecord implements IHolyCubeRecord {
	final IHolyRecord cellsetRecord;
	final IHolyRecord aggregateTableRecord;

	public HolyCubeRecord(IHolyRecord cellsetRecord, IHolyRecord aggregateTableRecord) {
		this.cellsetRecord = cellsetRecord;
		this.aggregateTableRecord = aggregateTableRecord;
	}

	@Override
	public IHolyRecord getCellsetRecord() {
		return cellsetRecord;
	}

	@Override
	public IHolyRecord getAggregateTableRecord() {
		return aggregateTableRecord;
	}

	@Override
	public String toString() {
		ToStringHelper toStringhelper = MoreObjects.toStringHelper(this);

		toStringhelper.add("cell", cellsetRecord.toString());
		toStringhelper.add("measures", aggregateTableRecord.toString());

		return toStringhelper.toString();
	}
}
