package eu.solven.holymolap.it.nyc;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.ImmutableSortedMap;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.serialization.HolyKryoHelper;
import eu.solven.holymolap.sink.arrow.LoadFromArrow;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;

public class ITLoadNycTaxiRides_Arrow_Single {

	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadNycTaxiRides_Arrow_Single.class);

	// TODO Enable passenger_count as axes (in addition of .SUM)
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	// Starts with '--add-opens=java.base/java.nio=ALL-UNNAMED -Djol.magicFieldOffset=true'
	public static void main(String[] args) throws IOException {
		Path tmpFile;

		{
			// https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
			String uri = "file://"
					+ "/var/folders/8b/p64c8tfs4d7gf3v8tcmwbz580000gn/T/holymolap-nyc-7061929697759030099.parquet";
			LOGGER.info("About to copy locally {}", uri);
			// parquetLength = Files.copy(new URL(uri).openStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
			tmpFile = Paths.get(uri);
		}
		if (!tmpFile.toFile().isFile()) {
			// LOGGER.warn("Failure relying on cache. We download again the file", e);
			String uri = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet";

			tmpFile = Files.createTempFile("holymolap-nyc-", ".parquet");
			LOGGER.info("About to copy {} to {}", uri, tmpFile);
			Files.copy(new URL(uri).openStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}
		long parquetLength = tmpFile.toFile().length();

		LOGGER.info("Copied locally into {} size={}", tmpFile, PepperLogHelper.humanBytes(parquetLength));

		LoadResult loadResult = new LoadFromArrow() {
			@Override
			protected IHolyMeasuresDefinition defineMeasures(Schema schema) {
				LOGGER.info("Schema for {}: {}", schema, schema);
				return ITLoadNycTaxiRides_Arrow_Single.defineMeasures(schema);
			}
		}.loadParquetFile(tmpFile.toUri());

		Path cacheFolder = Paths.get("/Users/blacelle/Downloads/holymolap-cache-nyc");

		Kryo kryo = HolyKryoHelper.kryoForHolyCube();
		// kryo.register(Text.class);
		HolyKryoHelper.storePut(kryo, cacheFolder, tmpFile.getFileName().toString(), loadResult.getHolyCube());

		IHolyCube holyCube = loadResult.getHolyCube();
		long sizeInBytes = holyCube.getSizeInBytes();
		long deepSize = PepperFootprintHelper.deepSize(holyCube);
		LOGGER.info("Parquet.length={} is represented by holyCube.length={} holyCube.deepSize={}",
				PepperLogHelper.humanBytes(parquetLength),
				PepperLogHelper.humanBytes(sizeInBytes),
				PepperLogHelper.humanBytes(deepSize));

		checkQueries(loadResult);
	}

	private static void checkQueries(LoadResult loadResult) {
		IHolyCube holyCube = loadResult.getHolyCube();

		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(holyCube, countRecords);
			LOGGER.info("Total records: {}", result);

			Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(loadResult.getNumRows());
		}

		{
			String wildcard = "VendorID";
			NavigableMap<? extends NavigableMap<String, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(holyCube,
							AggregateQueryBuilder.edit(countRecords).addWildcard(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);

			Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, 1L))).isEqualTo(742_273L);
			Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, 2L))).isEqualTo(1_716_059L);
			Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, 5L))).isEqualTo(36L);
			Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, 6L))).isEqualTo(5_563L);
		}
	}

	public static IHolyMeasuresDefinition defineMeasures(Schema schema) {
		List<IMeasuredAxis> measuredAxes = schema.getFields()
				.stream()
				.filter(cd -> !("RatecodeID".equals(cd.getName())) && !(cd.getName().endsWith("_type")))
				.filter(cd -> ArrowTypeID.FloatingPoint == cd.getType().getTypeID())
				.map(cd -> new MeasuredAxis(cd.getName(), IStandardOperators.SUM))
				.collect(Collectors.toCollection(ArrayList::new));

		Assertions.assertThat(measuredAxes)
				.hasSizeBetween(5, 11)
				// .contains(new MeasuredAxis("passenger_count", IStandardOperators.SUM))
				.doesNotContain(new MeasuredAxis("RatecodeID", IStandardOperators.SUM));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURED_AXIS);

		IHolyMeasuresDefinition measures = new HolyMeasuresTableDefinition(measuredAxes);
		return measures;
	}
}
