package eu.solven.holymolap.sink.record;

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

}
