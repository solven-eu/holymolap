package eu.solven.holymolap.sink.reduce;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;

/**
 * Converts a {@link Map} into a {@link IHolyRecord}
 * 
 * @author Benoit Lacelle
 *
 */
public class SlowMapHolyRecord implements IHolyRecord {
	private final List<String> axes;
	private final Map<String, ?> coordinates;

	public SlowMapHolyRecord(Map<String, ?> coordinates) {
		this.axes = ImmutableList.copyOf(coordinates.keySet());
		this.coordinates = coordinates;
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int i = 0; i < axes.size(); i++) {
			String axis = axes.get(i);
			Object coordinate = coordinates.get(axis);

			if (coordinate instanceof Double) {
				visitor.onDouble(i, ((Double) coordinate).doubleValue());
			} else {
				visitor.onObject(i, coordinate);
			}
		}
	}
}