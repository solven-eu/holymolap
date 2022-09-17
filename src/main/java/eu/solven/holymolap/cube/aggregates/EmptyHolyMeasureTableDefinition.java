package eu.solven.holymolap.cube.aggregates;

import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.stable.v1.IAggregatedAxis;

public class EmptyHolyMeasureTableDefinition implements IHolyMeasuresTableDefinition {

	@Override
	public List<IHolyMeasureColumnMeta> measures() {
		return Collections.emptyList();
	}

	@Override
	public int getMeasureIndex(IHolyMeasureColumnMeta aggregation) {
		return -1;
	}

	@Override
	public int findMeasureIndex(IAggregatedAxis aggregatesAxis) {
		return -1;
	}

}
