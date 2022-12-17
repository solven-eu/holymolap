package eu.solven.holymolap.mvc;

import java.util.Map;
import java.util.NavigableMap;

import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.query.AggregationToMapHelper;
import eu.solven.holymolap.stable.beta.IOneShotAggregator;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class OneShotAggregator implements IOneShotAggregator {

	@Override
	public NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> aggregate(ICompositeHolyCube cube,
			IAggregationQuery query) {
		return AggregationToMapHelper.measuresToNavigableMap(cube, query);
	}

}
