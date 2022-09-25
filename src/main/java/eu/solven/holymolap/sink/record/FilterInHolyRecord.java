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
					visitor.onObject(axisIndex, visitor);
				}
			}
		});
	}

	@Override
	public List<String> getAxes() {
		return underlying.getAxes();
	}

	@Override
	public int[] objectIndexes() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getObject(int axisIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int[] intIndexes() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getInt(int axisIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int[] doubleIndexes() {
		throw new UnsupportedOperationException();
	}

	@Override
	public double getDouble(int axisIndex) {
		throw new UnsupportedOperationException();
	}

}
