package eu.solven.holymolap.sink.reduce;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

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
public class MapHolyMeasuresRecord implements IHolyMeasuresRecord {
	private final List<IMeasuredAxis> measuredAxes;
	private final Map<IMeasuredAxis, ?> coordinates;

	public MapHolyMeasuresRecord(Map<IMeasuredAxis, ?> coordinates) {
		this.measuredAxes = ImmutableList.copyOf(coordinates.keySet());
		this.coordinates = coordinates;
	}

	@Override
	public List<IMeasuredAxis> getMeasuredAxes() {
		return measuredAxes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < measuredAxes.size(); i++) {
			IMeasuredAxis axis = measuredAxes.get(i);
			Object coordinate = coordinates.get(axis);

			if (coordinate instanceof Double) {
				visitor.onDouble(i, ((Double) coordinate).doubleValue());
			} else {
				// tpep_pickup_datetime
				visitor.onObject(i, coordinate);
			}
		}
	}
}