package eu.solven.holymolap.sink.record;

import java.util.List;
import java.util.Set;

public class FilterInHolyRecord implements IHolyRecord {
	final IHolyRecord underlying;
	final Set<String> filteredIn;

	public FilterInHolyRecord(IHolyRecord underlying, Set<String> filteredIn) {
		this.underlying = underlying;
		this.filteredIn = filteredIn;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		underlying.accept(new IHolyRecordVisitor() {

			@Override
			public void onObject(int axisIndex, Object o) {
				String axis = underlying.getAxes().get(axisIndex);

				if (filteredIn.contains(axis)) {
					visitor.onObject(axisIndex, o);
				}
			}
		});
	}

	@Override
	public List<String> getAxes() {
		return underlying.getAxes();

		// TODO If we filter the relevant axes, we need to adjust the indexes in .accept
		// return underlying.getAxes().stream().filter(s -> filteredIn.contains(s)).collect(Collectors.toList());
	}

}
