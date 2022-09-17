package eu.solven.holymolap.query;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasAggregations;
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
		return new AxesFilterAnd(Collections.emptyMap());
	}

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public List<IMeasuredAxis> getAggregations() {
		return Collections.emptyList();
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
