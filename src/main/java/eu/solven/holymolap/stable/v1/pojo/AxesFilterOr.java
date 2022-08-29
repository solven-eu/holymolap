package eu.solven.holymolap.stable.v1.pojo;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAnd;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterOr;

/**
 * Default implementation for {@link IAxesFilterAnd}
 * 
 * @author Benoit Lacelle
 *
 */
public class AxesFilterOr implements IAxesFilterOr {

	final List<IAxesFilter> filters;

	public AxesFilterOr(List<IAxesFilter> filters) {
		this.filters = ImmutableList.copyOf(filters);
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty OR is considered to match nothing
		return !filters.isEmpty();
	}

	@Override
	public boolean isOr() {
		return true;
	}

	@Override
	public List<IAxesFilter> getOr() {
		return filters;
	}

}
