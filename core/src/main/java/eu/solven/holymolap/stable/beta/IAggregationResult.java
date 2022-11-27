package eu.solven.holymolap.stable.beta;

import java.util.Map;
import java.util.NavigableMap;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * The result of an {@link IAggregationQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregationResult {

	NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> getAsMap();
}
