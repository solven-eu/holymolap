package eu.solven.holymolap.it.nyc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.serialization.HolyKryoHelper;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.avro.AvroHolyRecord;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class ITLoadNycTaxiRides_Multiple {
	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Multiple.class);

	// TODO Enable passenger_count as axes
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	public static void main(String[] args) throws Exception {
		// // https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
		Path inputFolder = Paths.get("/Users/blacelle/Downloads/2021-yellow");
		Path cacheFolder = Paths.get("/Users/blacelle/Downloads/2021-yellow-cache");

		AtomicLong recordsCount = new AtomicLong();

		// https://stackoverflow.com/questions/33596618/how-can-i-get-a-parallel-stream-of-files-walk
		List<Path> collectToForceParallel =
				Files.walk(inputFolder).filter(p -> p.toFile().isFile()).collect(Collectors.toList());

		long totalSizeOnDisk = collectToForceParallel.stream().mapToLong(p -> p.toFile().length()).sum();
		LOGGER.info("ABout to load {} files for a total of {}",
				collectToForceParallel.size(),
				PepperLogHelper.humanBytes(totalSizeOnDisk));

		Kryo kryo = HolyKryoHelper.kryoForHolyCube();

		kryo.register(Utf8.class);

		collectToForceParallel.stream()
				.parallel()
				// .filter(path -> {
				// String pathAsString = path.toString();
				// if (null != store.get(pathAsString)) {
				// LOGGER.info("We have already {} in cache", pathAsString);
				// return false;
				// } else {
				// return true;
				// }
				// })
				.map(path -> {
					IHolyCube holyCube = loadHolyCube(recordsCount, path);

					long parquetLength = path.toFile().length();
					long sizeInBytes = holyCube.getSizeInBytes();
					long deepSize = PepperFootprintHelper.deepSize(holyCube);
					LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
							PepperLogHelper.humanBytes(parquetLength),
							PepperLogHelper.humanBytes(sizeInBytes),
							PepperLogHelper.humanBytes(deepSize));

					// byte barray[] = conf.asByteArray(holyCube);

					String pathAsString = path.toString();
					return Maps.immutableEntry(pathAsString, holyCube);
				})
				.forEach(e -> HolyKryoHelper.storePut(kryo, cacheFolder, e.getKey(), e.getValue()));

		Map<String, IHolyCube> partitions = collectToForceParallel.stream()
				.map(p -> p.toString())
				.collect(Collectors.toMap(pathAsString -> pathAsString,
						pathAsString -> HolyKryoHelper.storeGet(kryo, cacheFolder, pathAsString)));

		ICompositeHolyCube partitionnedCube = new CompositeHolyCube(partitions);

		executeQueries(recordsCount, partitionnedCube);
	}

	private static IHolyCube loadHolyCube(AtomicLong recordsCount, Path path) {
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

			List<String> axes = AvroHolyRecord.allAxes(schema);
			IHolyMeasuresDefinition measures = ITLoadNycTaxiRides_Single_Avro.defineMeasures(schema);
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

				sink.sink(new AvroHolyRecord(axes, nextRecord));
			}

			holyCube = sink.closeToHolyCube();

		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return holyCube;
	}

	private static void executeQueries(AtomicLong recordsCount, ICompositeHolyCube partitionnedCube) {
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

}
