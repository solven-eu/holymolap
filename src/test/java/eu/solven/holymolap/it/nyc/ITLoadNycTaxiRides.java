package eu.solven.holymolap.it.nyc;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasureTableDefinition;
import eu.solven.holymolap.measures.operator.IOperatorFactory;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;

public class ITLoadNycTaxiRides {
	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides.class);

	public static void main(String[] args) throws IOException {
		// https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
		String uri = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet";

		Path tmpFile = Files.createTempFile("holymolap-nyc-", ".parquet");

		LOGGER.info("About to copy locally {}", uri);
		long parquetLength = Files.copy(new URL(uri).openStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
		LOGGER.info("Copied locally into {} size={}", tmpFile, PepperLogHelper.humanBytes(parquetLength));

		Configuration hadoopConf = new Configuration();
		HadoopInputFile hadoopFile =
				HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(tmpFile.toUri()), hadoopConf);

		ParquetFileReader readFooter = ParquetFileReader.open(hadoopFile);
		MessageType schema = readFooter.getFileMetaData().getSchema();

		List<String> axes = schema.getColumns()
				.stream()
				.map(cd -> Stream.of(cd.getPath()).collect(Collectors.joining(".")))
				.collect(Collectors.toList());
		List<IMeasuredAxis> measuredAxes = schema.getColumns()
				.stream()
				.filter(cd -> "double".equals(cd.getPrimitiveType().getPrimitiveTypeName().name()))
				.map(cd -> new MeasuredAxis(Stream.of(cd.getPath()).collect(Collectors.joining(".")),
						IOperatorFactory.SUM))
				.collect(Collectors.toList());

		IHolyMeasuresDefinition measures = new HolyMeasureTableDefinition(measuredAxes);
		LOGGER.info("Measures: {}", measures);

		ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopFile).build();

		HolyCubeSink sink = new HolyCubeSink(measures);

		long nbInsert = 0;
		while (true) {
			GenericRecord nextRecord = reader.read();

			if (nextRecord == null) {
				break;
			}

			if (nbInsert == 0) {
				LOGGER.info("First record: {}", nextRecord);
			}
			nbInsert++;

			sink.sink(convertAvroToHoly(axes, nextRecord));
		}

		IHolyCube holyCube = sink.closeToHolyCube();

		long sizeInBytes = holyCube.getSizeInBytes();
		LOGGER.info("Parquet.length={} is repsented by holyCube.length={}",
				PepperLogHelper.humanBytes(parquetLength),
				PepperLogHelper.humanBytes(sizeInBytes));

		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();
	}

	private static IHolyRecord convertAvroToHoly(List<String> axes, GenericRecord nextRecord) {
		return new IHolyRecord() {

			@Override
			public List<String> getAxes() {
				return axes;
			}

			@Override
			public void accept(IHolyRecordVisitor visitor) {
				List<Field> fields = nextRecord.getSchema().getFields();
				for (int i = 0; i < fields.size(); i++) {
					visitor.onObject(i, nextRecord.get(i));
				}
			}
		};
	}
}
