package eu.solven.holymolap.sink.record;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class CellsetToMeasuresHolyRecord implements IHolyMeasuresRecord {
	final List<IMeasuredAxis> measuredAxes;
	final Set<String> measuredColumns;

	final IHolyRecord underlying;

	public CellsetToMeasuresHolyRecord(IHolyRecord underlying, IHolyMeasuresDefinition measures) {
		this.underlying = underlying;

		measuredAxes =
				measures.measures().stream().map(IHolyMeasureColumnMeta::asMeasuredAxis).collect(Collectors.toList());

		measuredColumns = measuredAxes.stream().map(ma -> ma.getAxis()).collect(Collectors.toSet());
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		underlying.accept(new IHolyRecordVisitor() {

			@Override
			public void onObject(int axisIndex, Object o) {
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
	public List<IMeasuredAxis> getMeasuredAxes() {
		return measuredAxes;

		// TODO If we filter the relevant axes, we need to adjust the indexes in .accept
		// return underlying.getAxes().stream().filter(s -> filteredIn.contains(s)).collect(Collectors.toList());
	}

}
