package eu.solven.holymolap.stable.beta;

import java.util.Map;
import java.util.NavigableMap;

import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * This interface enables simplest queries. It is synchronous, hence should be reserved for simple queries.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IOneShotAggregator {
	NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> aggregate(ICompositeHolyCube holyCube,
			IAggregationQuery query);
}
