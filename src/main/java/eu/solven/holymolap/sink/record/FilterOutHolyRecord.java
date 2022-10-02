package eu.solven.holymolap.sink.record;

import java.util.List;
import java.util.Set;

public class FilterOutHolyRecord implements IHolyRecord {
	final IHolyRecord underlying;
	final Set<String> filteredOut;

	public FilterOutHolyRecord(IHolyRecord underlying, Set<String> filteredOut) {
		this.underlying = underlying;
		this.filteredOut = filteredOut;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		underlying.accept(new IHolyRecordVisitor() {

			@Override
			public void onObject(int axisIndex, Object o) {
				String axis = underlying.getAxes().get(axisIndex);

				if (!filteredOut.contains(axis)) {
					visitor.onObject(axisIndex, o);
				}
			}
		});
	}

	@Override
	public List<String> getAxes() {
		return underlying.getAxes();
	}
}
