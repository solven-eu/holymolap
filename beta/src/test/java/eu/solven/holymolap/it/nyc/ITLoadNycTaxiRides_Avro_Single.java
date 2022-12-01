package eu.solven.holymolap.it.nyc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.avro.AvroHolyRecord;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class ITLoadNycTaxiRides_Avro_Single {

	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Avro_Single.class);

	// TODO Enable passenger_count as axes
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	public static void main(String[] args) throws IOException {
		Path tmpFile = Files.createTempFile("holymolap-nyc-", ".parquet");

		long parquetLength;
		// https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
		try {
			String uri =
					"file:///var/folders/8b/p64c8tfs4d7gf3v8tcmwbz580000gn/T/holymolap-nyc-3336560945066608729.parquet";
			LOGGER.info("About to copy locally {}", uri);
			parquetLength = Files.copy(new URL(uri).openStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
		} catch (FileNotFoundException e) {
			LOGGER.warn("Failure relying on cache. We doanload again the file", e);

			String uri = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet";
			LOGGER.info("About to copy locally {}", uri);
			parquetLength = Files.copy(new URL(uri).openStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}
		LOGGER.info("Copied locally into {} size={}", tmpFile, PepperLogHelper.humanBytes(parquetLength));

		Configuration hadoopConf = new Configuration();
		HadoopInputFile hadoopFile =
				HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(tmpFile.toUri()), hadoopConf);

		ParquetFileReader readFooter = ParquetFileReader.open(hadoopFile);

		long recordCount = readFooter.getFilteredRecordCount();
		LOGGER.info("RecordCount: {}", recordCount);

		MessageType schema = readFooter.getFileMetaData().getSchema();

		List<String> avroAxes = AvroHolyRecord.allAxes(schema);
		IHolyMeasuresDefinition measures = defineMeasures(schema);
		LOGGER.info("Measures: {}", measures);

		ParquetReader<GenericRecord> avroReader = AvroParquetReader.<GenericRecord>builder(hadoopFile).build();

		HolyCubeSink sink = new HolyCubeSink(measures);

		long nbInsert = 0;
		while (true) {
			GenericRecord nextRecord = avroReader.read();

			if (nextRecord == null) {
				break;
			}

			if (nbInsert == 0) {
				LOGGER.info("First record: {}", nextRecord);
			}
			nbInsert++;

			IHolyRecord holyRecord = convertAvroToHoly(avroAxes, nextRecord);
			sink.sink(holyRecord);
		}

		IHolyCube holyCube = sink.closeToHolyCube();

		long sizeInBytes = holyCube.getSizeInBytes();
		long deepSize = PepperFootprintHelper.deepSize(holyCube);
		LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
				PepperLogHelper.humanBytes(parquetLength),
				PepperLogHelper.humanBytes(sizeInBytes),
				PepperLogHelper.humanBytes(deepSize));

		// GraphLayout.parseInstance(holyCube).toImage(tmpFile.toString() + ".graph.jpg");

		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(holyCube, countRecords);
			LOGGER.info("Total records: {}", result);

			Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(recordCount);
		}

		{
			String wildcard = "VendorID";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(holyCube,
					AggregateQueryBuilder.edit(countRecords).addWildcards(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);
		}
	}

	private static IHolyRecord convertAvroToHoly(List<String> axes, GenericRecord nextRecord) {
		return new AvroHolyRecord(axes, nextRecord);
	}

	public static IHolyMeasuresDefinition defineMeasures(MessageType schema) {
		List<IMeasuredAxis> measuredAxes = schema.getColumns()
				.stream()
				.filter(cd -> !(cd.getPath().length == 1 && "RatecodeID".equals(cd.getPath()[0])))
				.filter(cd -> PrimitiveTypeName.DOUBLE == cd.getPrimitiveType().getPrimitiveTypeName())
				.map(cd -> new MeasuredAxis(Stream.of(cd.getPath()).collect(Collectors.joining(".")),
						IStandardOperators.SUM))
				.collect(Collectors.toCollection(ArrayList::new));
		
		Assertions.assertThat(measuredAxes)
				.hasSize(11)
				.contains(new MeasuredAxis("passenger_count", IStandardOperators.SUM))
				.doesNotContain(new MeasuredAxis("RatecodeID", IStandardOperators.SUM));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURED_AXIS);

		IHolyMeasuresDefinition measures = new HolyMeasuresTableDefinition(measuredAxes);
		return measures;
	}
}
