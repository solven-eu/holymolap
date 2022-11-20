package eu.solven.holymolap.stable.beta;

import java.util.Map;
import java.util.NavigableMap;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * A simple {@link IAggregationResult} holding the human-friendly {@link NavigableMap}
 * 
 * @author Benoit Lacelle
 *
 */
public class MapAggregationResult implements IAggregationResult {

	final NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> asMap;

	public MapAggregationResult(NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> asMap) {
		this.asMap = asMap;
	}

	@Override
	public NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> getAsMap() {
		return asMap;
	}

}
