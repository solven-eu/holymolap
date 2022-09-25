package eu.solven.holymolap.cube.measures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class HolyMeasureTableDefinition implements IHolyMeasuresDefinition {
	final List<IMeasuredAxis> measuredAxes;
	final List<IHolyMeasureColumnMeta> measuredMeta;

	public HolyMeasureTableDefinition(List<IMeasuredAxis> measuredAxes) {
		this.measuredAxes = measuredAxes;

		measuredMeta = new ArrayList<>(measuredAxes.size());
		for (int measureIndex = 0; measureIndex < measuredAxes.size(); measureIndex++) {
			IMeasuredAxis measuredAxis = measuredAxes.get(measureIndex);
			measuredMeta.add(new HolyMeasureColumnMeta(measuredAxis, measureIndex));
		}
	}

	public static IHolyMeasuresDefinition of(IMeasuredAxis... measures) {
		return new HolyMeasureTableDefinition(Arrays.asList(measures));
	}

	@Override
	public List<IHolyMeasureColumnMeta> measures() {
		return Collections.unmodifiableList(measuredMeta);
	}

	@Override
	public int findMeasureIndex(IMeasuredAxis measuredAxis) {
		return measuredAxes.indexOf(measuredAxis);
	}

}
