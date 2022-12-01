package eu.solven.holymolap.sink.reduce;

import java.util.List;

import eu.solven.holymolap.sink.record.IHolyMeasuresRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * Converts a pair of {@link List} into a {@link IHolyRecord}
 * 
 * @author Benoit Lacelle
 *
 */
public class ListHolyMeasuresRecord implements IHolyMeasuresRecord {
	private final List<IMeasuredAxis> measuredAxes;
	private final List<?> coordinates;

	public ListHolyMeasuresRecord(List<IMeasuredAxis> measuredAxes, List<?> coordinates) {
		this.measuredAxes = measuredAxes;
		this.coordinates = coordinates;

		if (measuredAxes.size() != coordinates.size()) {
			throw new IllegalArgumentException(measuredAxes.size() + " != " + coordinates.size());
		}
	}

	@Override
	public List<IMeasuredAxis> getMeasuredAxes() {
		return measuredAxes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < measuredAxes.size(); i++) {
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