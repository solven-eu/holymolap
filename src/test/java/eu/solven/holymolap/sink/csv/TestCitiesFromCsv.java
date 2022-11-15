package eu.solven.holymolap.sink.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSortedMap;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.csv.CsvHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;
import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;

public class TestCitiesFromCsv {

	private static final Logger LOGGER = LoggerFactory.getLogger(TestCitiesFromCsv.class);

	final Resource csvResource = new ClassPathResource("/examples/csv/cities.csv");

	// We allow emptyLines as the file has trailing empty rows
	final CsvSpecs specs = CsvSpecs.builder().ignoreEmptyLines(true).build();

	@Test
	public void testCitiesFromCsv() {
		try (InputStream is = csvResource.getInputStream()) {
			final CsvReader.Result csvResult = CsvReader.read(specs, is, SinkFactory.arrays());
			final long numRows = csvResult.numRows();
			LOGGER.info("Done parsing {} rows from a CSV file with length={}",
					PepperLogHelper.humanBytes(numRows),
					PepperLogHelper.humanBytes(csvResource.contentLength()));

			String csvDescription = Stream.of(csvResult.columns())
					.map(rc -> rc.name() + "->" + rc.dataType())
					.collect(Collectors.joining(", "));
			LOGGER.info("CSV structure: {}", csvDescription);

			List<String> axes = Stream.of(csvResult.columns()).map(rc -> rc.name()).collect(Collectors.toList());

			// Enable querying COUNT(*)
			List<IMeasuredAxis> measuredAxes = Arrays.asList(ICountMeasuresConstants.COUNT_MEASURED_AXIS);
			IHolyMeasuresDefinition measures = new HolyMeasuresTableDefinition(measuredAxes);
			LOGGER.info("Measures: {}", measures);

			Set<String> measuredAxesNames =
					measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

			HolyCubeSink sink = new HolyCubeSink(measures);

			{
				IHolyRecordsTable cellsTable =
						new CsvHolyRecordsTable(axes, numRows, csvResult, Predicates.not(measuredAxesNames::contains));
				IHolyRecordsTable measuresTable =
						new CsvHolyRecordsTable(axes, numRows, csvResult, measuredAxesNames::contains);
				sink.sink(cellsTable, measuresTable);
			}

			IHolyCube holyCube = sink.closeToHolyCube();
			LOGGER.info("We have an immutable cube ready for querying");

			long sizeInBytes = holyCube.getSizeInBytes();
			long deepSize = PepperFootprintHelper.deepSize(holyCube);
			LOGGER.info("CSV.length={} is represented by holyCube.length={} holyCube.deepSize={}",
					PepperLogHelper.humanBytes(csvResource.contentLength()),
					PepperLogHelper.humanBytes(sizeInBytes),
					PepperLogHelper.humanBytes(deepSize));

			{
				// Check eachCSV columns are an axis
				Assertions.assertThat(holyCube.getCellSet().getAxesWithCoordinates().axes())
						.containsExactlyElementsOf(Stream.of(csvResult.columns())
								.map(rc -> rc.name())
								.sorted()
								.collect(Collectors.toList()));

				SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

				{
					NavigableMap<? extends NavigableMap<?, ?>, ?> result =
							AggregateHelper.singleMeasureToNavigableMap(holyCube, countRecords);
					LOGGER.info("Total records: {}", result);

					Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(numRows);
				}

				{
					String wildcard = "State";
					NavigableMap<? extends NavigableMap<?, ?>, ?> result =
							AggregateHelper.singleMeasureToNavigableMap(holyCube,
									AggregateQueryBuilder.edit(countRecords).addWildcard(wildcard).build());
					// LOGGER.info("Total records by '{}': {}", wildcard, result);

					Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, "AL"))).isEqualTo(2L);
					Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, "AZ"))).isEqualTo(1L);
					Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, "KS"))).isEqualTo(3L);
					Assertions.assertThat(result.get(ImmutableSortedMap.of(wildcard, "WA"))).isEqualTo(6L);
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException("Issue processing " + csvResource, e);
		} catch (CsvReaderException e) {
			throw new RuntimeException("Issue processing " + csvResource, e);
		}
	}

}
