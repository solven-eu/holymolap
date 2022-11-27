package eu.solven.holymolap.stable.v1.pojo;

import java.util.Set;

import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAxisEquals;

public class AxisEqualsFilter implements IAxesFilterAxisEquals {

	final String axis;
	final Object filtered;

	public AxisEqualsFilter(String axis, Object filtered) {
		if (Set.of(ICountMeasuresConstants.STAR).contains(axis)) {
			throw new IllegalArgumentException("Invalid axis for filter: " + axis);
		}

		this.axis = axis;
		this.filtered = filtered;

		if (filtered == null) {
			throw new IllegalArgumentException("'filtered' can not be null");
		} else if (filtered instanceof IAxesFilter) {
			throw new IllegalArgumentException("'filtered' can not be a: " + IAxesFilter.class.getSimpleName());
		}
	}

	@Override
	public boolean isExclusion() {
		return false;
	}

	@Override
	public boolean isAxisEquals() {
		return true;
	}

	@Override
	public String getAxis() {
		return axis;
	}

	@Override
	public Object getFiltered() {
		return filtered;
	}

	@Override
	public String toString() {
		return "AxisEqualsFilter [axis=" + axis + ", filtered=" + filtered + "]";
	}

}
