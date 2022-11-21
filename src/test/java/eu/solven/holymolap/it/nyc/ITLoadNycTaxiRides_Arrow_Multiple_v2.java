package eu.solven.holymolap.it.nyc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.arrow.LoadFromArrow;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

// https://github.com/toddwschneider/nyc-taxi-data
// https://github.com/toddwschneider/nyc-taxi-data/tree/master/clickhouse
// brew install altinity/clickhouse/clickhouse
// brew install wget
// git clone git@github.com:toddwschneider/nyc-taxi-data.git
// cd nyc-taxi-data
// ./download_raw_data.sh
public class ITLoadNycTaxiRides_Arrow_Multiple_v2 {
	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Arrow_Multiple_v2.class);

	// Starts with '--add-opens=java.base/java.nio=ALL-UNNAMED -Djol.magicFieldOffset=true'
	public static void main(String[] args) throws Exception {
		// // https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
		Path inputFolder = Paths.get("/Users/blacelle/workspace3/nyc-taxi-data/data");

		AtomicLong recordsCount = new AtomicLong();

		// https://stackoverflow.com/questions/33596618/how-can-i-get-a-parallel-stream-of-files-walk
		List<Path> collectToForceParallel = Files.walk(inputFolder)
				.filter(p -> p.toFile().isFile())
				.filter(p -> p.getFileName().toString().startsWith("green_tripdata_")
						&& p.getFileName().toString().endsWith(".parquet"))
				.collect(Collectors.toList());

		long totalSizeOnDisk = collectToForceParallel.stream().mapToLong(p -> p.toFile().length()).sum();
		LOGGER.info("ABout to load {} files for a total of {}",
				collectToForceParallel.size(),
				PepperLogHelper.humanBytes(totalSizeOnDisk));

		LoadingContext loadingContext = new LoadingContext(inputFolder.toString());

		Map<String, IHolyCube> partitions = collectToForceParallel.stream().limit(2).parallel().map(path -> {
			LoadResult loadResult = new LoadFromArrow(loadingContext) {
				@Override
				protected IHolyMeasuresDefinition defineMeasures(Schema schema) {
					return ITLoadNycTaxiRides_Arrow_Single.defineMeasures(schema);
				}
			}.loadParquetFile(path.toUri());

			recordsCount.addAndGet(loadResult.getNumRows());

			IHolyCube holyCube = loadResult.getHolyCube();

			long parquetLength = path.toFile().length();
			long sizeInBytes = holyCube.getSizeInBytes();
			long deepSize = PepperFootprintHelper.deepSize(holyCube);
			LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
					PepperLogHelper.humanBytes(parquetLength),
					PepperLogHelper.humanBytes(sizeInBytes),
					PepperLogHelper.humanBytes(deepSize));

			String pathAsString = path.toString();
			return Maps.immutableEntry(pathAsString, holyCube);
		}).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		ICompositeHolyCube partitionnedCube = new CompositeHolyCube(partitions);

		executeQueries(recordsCount, partitionnedCube);
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
