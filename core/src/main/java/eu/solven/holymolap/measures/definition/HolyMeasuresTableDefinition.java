package eu.solven.holymolap.measures.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.measures.HolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class HolyMeasuresTableDefinition implements IHolyMeasuresDefinition {
	final List<IMeasuredAxis> measuredAxes;
	final List<IHolyMeasureColumnMeta> measuredMeta;

	protected HolyMeasuresTableDefinition() {
		this(Collections.emptyList());
	}

	public HolyMeasuresTableDefinition(List<IMeasuredAxis> measuredAxes) {
		this.measuredAxes = new ArrayList<>(measuredAxes);

		measuredMeta = new ArrayList<>(measuredAxes.size());
		for (int measureIndex = 0; measureIndex < measuredAxes.size(); measureIndex++) {
			IMeasuredAxis measuredAxis = measuredAxes.get(measureIndex);
			measuredMeta.add(new HolyMeasureColumnMeta(measuredAxis, measureIndex));
		}
	}

	public static IHolyMeasuresDefinition of(IMeasuredAxis... measures) {
		return new HolyMeasuresTableDefinition(Arrays.asList(measures));
	}

	@Override
	public List<IHolyMeasureColumnMeta> measures() {
		return Collections.unmodifiableList(measuredMeta);
	}

	@Override
	public int findMeasureIndex(IMeasuredAxis measuredAxis) {
		return measuredAxes.indexOf(measuredAxis);
	}

	@Override
	public String toString() {
		return "HolyMeasureTableDefinition [measuredAxes=" + measuredAxes + ", measuredMeta=" + measuredMeta + "]";
	}

	public static IHolyMeasuresDefinition withCountStar(List<IMeasuredAxis> measuredAxis) {
		List<IMeasuredAxis> withCountStar = new ArrayList<>();

		if (!measuredAxis.contains(ICountMeasuresConstants.COUNT_MEASURE)) {
			withCountStar.add(ICountMeasuresConstants.COUNT_MEASURE);
		}

		withCountStar.addAll(measuredAxis);

		return new HolyMeasuresTableDefinition(withCountStar);
	}

}
