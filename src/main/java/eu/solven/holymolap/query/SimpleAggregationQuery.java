package eu.solven.holymolap.query;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasAggregations;
import eu.solven.holymolap.stable.v1.IHasAxes;
import eu.solven.holymolap.stable.v1.IHasFilters;

/**
 * Simple {@link IAggregationQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class SimpleAggregationQuery implements IAggregationQuery {

	protected final IHasFilters axesFilters;
	protected final IHasAxes axes;
	protected final IHasAggregations hasAggregations;

	public SimpleAggregationQuery(IHasFilters hasFilters, IHasAxes axes, IHasAggregations hasAggregations) {
		this.axesFilters = hasFilters;
		this.axes = axes;
		this.hasAggregations = hasAggregations;
	}

	@Override
	public IAxesFilter getFilters() {
		return axesFilters.getFilters();
	}

	@Override
	public List<String> getAxes() {
		return axes.getAxes();
	}

	@Override
	public List<IMeasuredAxis> getAggregations() {
		return hasAggregations.getAggregations();
	}

	@Override
	public String toString() {
		// We call the getters to workaround usage of lambda
		return "SimpleAggregationQuery [axesFilters=" + axesFilters.getFilters()
				+ ", axes="
				+ axes.getAxes()
				+ ", hasAggregations="
				+ hasAggregations.getAggregations()
				+ "]";
	}

	@Override
	public IAggregationQuery addAggregations(IHasAggregations additionalAggregations) {
		IHasAggregations mergedAggregations = () -> ImmutableList.<IMeasuredAxis>builder()
				.addAll(getAggregations())
				.addAll(additionalAggregations.getAggregations())
				.build();

		return new SimpleAggregationQuery(this, this, mergedAggregations);
	}
}
