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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.HolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class ITLoadNycTaxiRides_Single_Arrow {

	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Single_Arrow.class);

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

		IHolyMeasuresDefinition measures = ITLoadNycTaxiRides_Single_Avro.defineMeasures(schema);
		LOGGER.info("Measures: {}", measures);

		Set<String> measuredAxes =
				measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

		HolyCubeSink sink = new HolyCubeSink(measures);

		// https://arrow.apache.org/cookbook/java/io.html#reading-parquet-file
		ScanOptions options = new ScanOptions(32768);
		try (BufferAllocator allocator = new RootAllocator();
				DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator,
						NativeMemoryPool.getDefault(),
						FileFormat.PARQUET,
						tmpFile.toUri().toString());
				Dataset dataset = datasetFactory.finish();
				Scanner scanner = dataset.newScan(options);
				ArrowReader reader = scanner.scanBatches()) {
			while (reader.loadNextBatch()) {
				try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
					Schema arrowSchema = root.getSchema();
					List<String> axes = new ArrayList<>(schema.getFields().size());
					for (Field field : arrowSchema.getFields()) {
						axes.add(field.getName());
					}

					// By default, we consider as cellAxes only if not a measure column
					boolean[] inCell = new boolean[axes.size()];
					boolean[] inMeasure = new boolean[axes.size()];

					for (int fieldIndex = 0; fieldIndex < axes.size(); fieldIndex++) {
						String axis = axes.get(fieldIndex);
						if (measuredAxes.contains(axis)) {
							inMeasure[fieldIndex] = true;
						} else {
							inCell[fieldIndex] = true;
						}
					}

					int rowCount = root.getRowCount();

					List<FieldVector> fieldVectors = root.getFieldVectors();
					for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
						IHolyRecord cellRecord = new ArrowSubsetHolyRecord(axes, inCell, fieldVectors, rowIndex);
						IHolyRecord measuresRecord = new ArrowSubsetHolyRecord(axes, inMeasure, fieldVectors, rowIndex);

						sink.sink(new HolyCubeRecord(cellRecord, measuresRecord));
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		IHolyCube holyCube = sink.closeToHolyCube();

		long sizeInBytes = holyCube.getSizeInBytes();
		long deepSize = PepperFootprintHelper.deepSize(holyCube);
		LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
				PepperLogHelper.humanBytes(parquetLength),
				PepperLogHelper.humanBytes(sizeInBytes),
				PepperLogHelper.humanBytes(deepSize));

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
					AggregateQueryBuilder.edit(countRecords).addWildcard(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);
		}
	}

}
