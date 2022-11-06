package eu.solven.holymolap.it.nyc;

import java.util.List;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;

public class ArrowSubsetHolyRecord implements IHolyRecord {
	private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSubsetHolyRecord.class);

	final List<String> axes;
	final boolean[] keepMe;
	final List<FieldVector> fieldVectors;

	final int rowIndex;

	public ArrowSubsetHolyRecord(List<String> axes, boolean[] keepMe, List<FieldVector> fieldVectors, int rowIndex) {
		this.axes = axes;
		this.keepMe = keepMe;
		this.fieldVectors = fieldVectors;

		this.rowIndex = rowIndex;
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		for (int fieldIndex = 0; fieldIndex < keepMe.length; fieldIndex++) {
			FieldVector fieldVector = fieldVectors.get(fieldIndex);

			if (fieldVector.isNull(rowIndex)) {
				LOGGER.debug("Skip null");
			} else if (fieldVector instanceof FloatingPointVector) {
				FloatingPointVector floatingVector = (FloatingPointVector) fieldVector;
				visitor.onDouble(fieldIndex, floatingVector.getValueAsDouble(rowIndex));
			} else if (fieldVector instanceof BaseIntVector) {
				BaseIntVector intVector = (BaseIntVector) fieldVector;
				visitor.onLong(fieldIndex, intVector.getValueAsLong(rowIndex));
			} else {
				visitor.onObject(fieldIndex, fieldVector.getObject(rowIndex));
			}
		}
	}

	@Override
	public String toString() {
		ToStringHelper stringBuilder = MoreObjects.toStringHelper(this);

		this.accept((i, o) -> {
			stringBuilder.add(axes.get(i), o);
		});

		return stringBuilder.toString();
	}
}
