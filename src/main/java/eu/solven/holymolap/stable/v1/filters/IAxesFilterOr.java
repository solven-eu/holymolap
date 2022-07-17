package eu.solven.holymolap.stable.v1.filters;

import java.util.List;

import eu.solven.holymolap.stable.v1.IAxesFilter;

public interface IAxesFilterOr extends IAxesFilter {

	/**
	 * Would throw if .isOr is false
	 * 
	 * @return
	 */
	List<IAxesFilterOr> getOr();
}
