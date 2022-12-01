package eu.solven.holymolap.sink.record;

import java.util.List;
import java.util.Set;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

public class FilterInMeasuresHolyRecord implements IHolyMeasuresRecord {
	final IHolyMeasuresRecord underlying;
	final Set<IMeasuredAxis> filteredIn;

	public FilterInMeasuresHolyRecord(IHolyMeasuresRecord underlying, Set<IMeasuredAxis> filteredIn) {
		this.underlying = underlying;
		this.filteredIn = filteredIn;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		underlying.accept(new IHolyRecordVisitor() {

			@Override
			public void onObject(int axisIndex, Object o) {
				IMeasuredAxis axis = underlying.getMeasuredAxes().get(axisIndex);

				if (filteredIn.contains(axis)) {
					visitor.onObject(axisIndex, o);
				}
			}
		});
	}

	@Override
	public List<IMeasuredAxis> getMeasuredAxes() {
		return underlying.getMeasuredAxes();

		// TODO If we filter the relevant axes, we need to adjust the indexes in .accept
		// return underlying.getAxes().stream().filter(s -> filteredIn.contains(s)).collect(Collectors.toList());
	}

}
