package eu.solven.holymolap.sink.record.avro;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.MessageType;

import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;

/**
 * Converts a {@link GenericRecord} into a {@link IHolyRecord}
 * 
 * @author Benoit Lacelle
 *
 */
public class AvroHolyRecord implements IHolyRecord {
	private final List<String> axes;
	private final GenericRecord nextRecord;

	public AvroHolyRecord(List<String> axes, GenericRecord nextRecord) {
		this.axes = axes;
		this.nextRecord = nextRecord;
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		List<Field> fields = nextRecord.getSchema().getFields();
		for (int i = 0; i < fields.size(); i++) {
			// Field field = fields.get(i);
			// if (field.schema().getLogicalType())
			Object coordinate = nextRecord.get(i);
			if (coordinate instanceof Double) {
				visitor.onDouble(i, ((Double) coordinate).doubleValue());
			} else {
				// tpep_pickup_datetime
				visitor.onObject(i, coordinate);
			}
		}
	}

	public static List<String> allAxes(MessageType schema) {
		return schema.getColumns()
				.stream()
				.map(cd -> Stream.of(cd.getPath()).collect(Collectors.joining(".")))
				.collect(Collectors.toList());
	}
}