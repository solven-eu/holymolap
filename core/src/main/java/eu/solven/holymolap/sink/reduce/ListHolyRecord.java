package eu.solven.holymolap.sink.reduce;

import java.util.List;

import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;

/**
 * Converts a pair of {@link List} into a {@link IHolyRecord}
 * 
 * @author Benoit Lacelle
 *
 */
public class ListHolyRecord implements IHolyRecord {
	private final List<String> axes;
	private final List<?> coordinates;

	public ListHolyRecord(List<String> axes, List<?> coordinates) {
		this.axes = axes;
		this.coordinates = coordinates;

		if (axes.size() != coordinates.size()) {
			throw new IllegalArgumentException(axes.size() + " != " + coordinates.size());
		}
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < axes.size(); i++) {
			// String axis = axes.get(i);
			Object coordinate = coordinates.get(i);

			if (coordinate instanceof Double) {
				visitor.onDouble(i, ((Double) coordinate).doubleValue());
			} else {
				// tpep_pickup_datetime
				visitor.onObject(i, coordinate);
			}
		}
	}
}