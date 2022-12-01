package eu.solven.holymolap.sink.csv;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.sink.record.IHolyMeasuresRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTableVisitor;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class DenormalizeHolyMeasuresRecordsTable implements IHolyMeasuresRecordsTable {
	private final IHolyRecordsTable underlying;

	final List<IMeasuredAxis> measuredAxes;
	final Set<String> measuredColumns;

	public DenormalizeHolyMeasuresRecordsTable(IHolyRecordsTable cellsetRecordsTable,
			IHolyMeasuresDefinition measuresDefinition) {
		this.underlying = cellsetRecordsTable;

		measuredAxes = measuresDefinition.measures()
				.stream()
				.map(IHolyMeasureColumnMeta::asMeasuredAxis)
				.collect(Collectors.toList());

		measuredColumns = measuredAxes.stream().map(ma -> ma.getAxis()).collect(Collectors.toSet());
	}

	@Override
	public List<IMeasuredAxis> getMeasures() {
		return measuredAxes;
	}

	@Override
	public void accept(IHolyRecordsTableVisitor visitor) {
		underlying.accept(new IHolyRecordsTableVisitor() {

			@Override
			public void onObject(int axisIndex, List<?> o) {
				String axis = underlying.getAxes().get(axisIndex);

				if (measuredColumns.contains(axis)) {
					for (int measuredAxisIndex = 0; measuredAxisIndex < measuredAxes.size(); measuredAxisIndex++) {
						IMeasuredAxis measuredAxis = measuredAxes.get(measuredAxisIndex);
						if (measuredAxis.getAxis().equals(axis)) {
							visitor.onObject(measuredAxisIndex, o);
						}
					}
				}
			}
		});
	}

	@Override
	public long size() {
		return underlying.size();
	}
}
