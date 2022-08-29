package eu.solven.holymolap.stable.v1.pojo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAnd;

/**
 * Default implementation for {@link IAxesFilterAnd}
 * 
 * @author Benoit Lacelle
 *
 */
public class AxesFilterAnd implements IAxesFilterAnd {

	final List<IAxesFilter> filters;

	public AxesFilterAnd(List<IAxesFilter> filters) {
		this.filters = filters;
	}

	public AxesFilterAnd(Map<String, ?> filters) {
		this.filters = filters.entrySet()
				.stream()
				.map(e -> new AxisEqualsFilter(e.getKey(), e.getValue()))
				.collect(Collectors.toList());
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty AND is considered to match everything
		return filters.isEmpty();
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public List<IAxesFilter> getAnd() {
		return Collections.unmodifiableList(filters);
	}

}
