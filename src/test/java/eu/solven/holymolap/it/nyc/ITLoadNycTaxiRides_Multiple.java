package eu.solven.holymolap.it.nyc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
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
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasureTableDefinition;
import eu.solven.holymolap.measures.operator.IOperatorFactory;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordVisitor;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class ITLoadNycTaxiRides_Multiple {
	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Multiple.class);

	// TODO Enable passenger_count as axes
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	public static void main(String[] args) throws IOException {
		// // https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
		Path folder = Paths.get("/Users/blacelle/Downloads/2021-yellow");

		AtomicLong recordsCount = new AtomicLong();

		// https://stackoverflow.com/questions/33596618/how-can-i-get-a-parallel-stream-of-files-walk
		List<Path> collectToForceParallel =
				Files.walk(folder).filter(p -> p.toFile().isFile()).collect(Collectors.toList());

		long totalSizeOnDisk = collectToForceParallel.stream().mapToLong(p -> p.toFile().length()).sum();
		LOGGER.info("ABout to load {} files for a total of {}",
				collectToForceParallel.size(),
				PepperLogHelper.humanBytes(totalSizeOnDisk));

		Map<String, IHolyCube> partitions = collectToForceParallel.stream().parallel().map(path -> {
			IHolyCube holyCube;
			try {
				Configuration hadoopConf = new Configuration();

				HadoopInputFile hadoopFile =
						HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), hadoopConf);

				//
				ParquetFileReader readFooter = ParquetFileReader.open(hadoopFile);

				long recordCount = readFooter.getFilteredRecordCount();
				LOGGER.info("RecordCount: {}", recordCount);
				recordsCount.addAndGet(recordCount);

				MessageType schema = readFooter.getFileMetaData().getSchema();

				List<String> axes = schema.getColumns()
						.stream()
						.map(cd -> Stream.of(cd.getPath()).collect(Collectors.joining(".")))
						.collect(Collectors.toList());
				IHolyMeasuresDefinition measures = defineMeasures(schema);
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

				holyCube = sink.closeToHolyCube();

			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}

			long parquetLength = path.toFile().length();
			long sizeInBytes = holyCube.getSizeInBytes();
			long deepSize = PepperFootprintHelper.deepSize(holyCube);
			LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
					PepperLogHelper.humanBytes(parquetLength),
					PepperLogHelper.humanBytes(sizeInBytes),
					PepperLogHelper.humanBytes(deepSize));

			return Maps.immutableEntry(path.toString(), holyCube);
		}).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		ICompositeHolyCube partitionnedCube = new CompositeHolyCube(partitions);

		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(partitionnedCube, countRecords);
			LOGGER.info("Total records: {}", result);

			Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(recordsCount.get());
		}

		{
			String wildcard = "VendorID";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(partitionnedCube,
							AggregateQueryBuilder.edit(countRecords).addWildcard(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);
		}
	}

	private static IHolyMeasuresDefinition defineMeasures(MessageType schema) {
		List<IMeasuredAxis> measuredAxes = schema.getColumns()
				.stream()
				.filter(cd -> !(cd.getPath().length == 1 && "RatecodeID".equals(cd.getPath()[0])))
				.filter(cd -> PrimitiveTypeName.DOUBLE == cd.getPrimitiveType().getPrimitiveTypeName())
				.map(cd -> new MeasuredAxis(Stream.of(cd.getPath()).collect(Collectors.joining(".")),
						IOperatorFactory.SUM))
				.collect(Collectors.toCollection(ArrayList::new));

		Assertions.assertThat(measuredAxes)
				.hasSize(11)
				.contains(new MeasuredAxis("passenger_count", IOperatorFactory.SUM))
				.doesNotContain(new MeasuredAxis("RatecodeID", IOperatorFactory.SUM));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURED_AXIS);

		IHolyMeasuresDefinition measures = new HolyMeasureTableDefinition(measuredAxes);
		return measures;
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
		};
	}
}
