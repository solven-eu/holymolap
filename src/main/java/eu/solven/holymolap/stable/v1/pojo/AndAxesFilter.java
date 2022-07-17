package eu.solven.holymolap.stable.v1.pojo;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAnd;

/**
 * Default implementation for {@link IAxesFilterAnd}
 * 
 * @author Benoit Lacelle
 *
 */
public class AndAxesFilter implements IAxesFilterAnd {

	final Map<String, ?> filters;

	public AndAxesFilter(Map<String, ?> filters) {
		this.filters = ImmutableMap.copyOf(filters);
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty Map is considering matching everything
		return filters.isEmpty();
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public Map<String, IAxesFilter> getAnd() {
		return filters.entrySet()
				.stream()
				.collect(Collectors.toMap(e -> e.getKey(), e -> new AxisEqualsFilter(e.getKey(), e.getValue())));
	}

}
