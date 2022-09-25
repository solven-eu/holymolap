package eu.solven.holymolap.query;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasMeasures;
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
	protected final IHasMeasures hasAggregations;

	public SimpleAggregationQuery(IHasFilters hasFilters, IHasAxes axes, IHasMeasures hasAggregations) {
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
	public List<IMeasuredAxis> getMeasures() {
		return hasAggregations.getMeasures();
	}

	@Override
	public String toString() {
		// We call the getters to workaround usage of lambda
		return "SimpleAggregationQuery [axesFilters=" + axesFilters.getFilters()
				+ ", axes="
				+ axes.getAxes()
				+ ", hasAggregations="
				+ hasAggregations.getMeasures()
				+ "]";
	}

	@Override
	public IAggregationQuery addAggregations(IHasMeasures additionalAggregations) {
		IHasMeasures mergedAggregations = () -> ImmutableList.<IMeasuredAxis>builder()
				.addAll(getMeasures())
				.addAll(additionalAggregations.getMeasures())
				.build();

		return new SimpleAggregationQuery(this, this, mergedAggregations);
	}
}
