package eu.solven.holymolap.stable.v1.filters;

import java.util.Map;

import eu.solven.holymolap.stable.v1.IAxesFilter;

public interface IAxesFilterAnd extends IAxesFilter {

	/**
	 * Would throw if .isAnd is false
	 * 
	 * @return
	 */
	Map<String, IAxesFilter> getAnd();
}
