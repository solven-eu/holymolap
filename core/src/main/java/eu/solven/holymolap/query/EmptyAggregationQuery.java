package eu.solven.holymolap.query;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasMeasures;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.AxesFilterAnd;

/**
 * Simple {@link IAggregationQuery}, where the filter is an AND condition.
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyAggregationQuery implements IAggregationQuery {

	@Override
	public IAxesFilter getFilters() {
		return new AxesFilterAnd(Collections.emptyList());
	}

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public List<IMeasuredAxis> getMeasures() {
		return Collections.emptyList();
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
