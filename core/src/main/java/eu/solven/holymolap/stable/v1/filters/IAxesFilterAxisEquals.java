package eu.solven.holymolap.stable.v1.filters;

import javax.annotation.Nonnull;

import eu.solven.holymolap.stable.v1.IAxesFilter;

public interface IAxesFilterAxisEquals extends IAxesFilter {

	@Nonnull
	String getAxis();

	@Nonnull
	Object getFiltered();
}
