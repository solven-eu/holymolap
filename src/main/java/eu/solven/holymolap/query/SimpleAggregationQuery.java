package eu.solven.holymolap.query;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasAxes;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.stable.v1.IHasMeasures;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * Simple {@link IAggregationQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class SimpleAggregationQuery implements IAggregationQuery {

	protected final IHasFilters axesFilters;
	protected final IHasAxes axes;
	protected final IHasMeasures hasMeasures;

	public SimpleAggregationQuery(IHasFilters hasFilters, IHasAxes axes, IHasMeasures hasMeasures) {
		this.axesFilters = hasFilters;
		this.axes = axes;
		this.hasMeasures = hasMeasures;
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
		return hasMeasures.getMeasures();
	}

	@Override
	public String toString() {
		// We call the getters to workaround usage of lambda
		return "SimpleAggregationQuery [axesFilters=" + axesFilters
				.getFilters() + ", axes=" + axes.getAxes() + ", hasMeasures=" + hasMeasures.getMeasures() + "]";
	}

	@Override
	public IAggregationQuery addAggregations(IHasMeasures additionalMeasures) {
		IHasMeasures mergedMeasures = () -> ImmutableList.<IMeasuredAxis>builder()
				.addAll(getMeasures())
				.addAll(additionalMeasures.getMeasures())
				.build();

		return new SimpleAggregationQuery(this, this, mergedMeasures);
	}
}
