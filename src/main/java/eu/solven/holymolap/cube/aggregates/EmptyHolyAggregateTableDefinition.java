package eu.solven.holymolap.cube.aggregates;

import java.util.Collections;
import java.util.List;

public class EmptyHolyAggregateTableDefinition implements IHolyAggregateTableDefinition {

	@Override
	public List<IHolyAggregatedColumnMeta> aggregations() {
		return Collections.emptyList();
	}

	@Override
	public int getAggregationIndex(IHolyAggregatedColumnMeta aggregation) {
		return -1;
	}

}
