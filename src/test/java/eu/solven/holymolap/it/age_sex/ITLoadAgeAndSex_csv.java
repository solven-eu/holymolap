package eu.solven.holymolap.it.age_sex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.csv.CsvHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;
import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.reading.CsvReader.ResultColumn;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;

public class ITLoadAgeAndSex_csv {

	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadAgeAndSex_csv.class);

	// TODO Enable passenger_count as axes
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	public static void main(String[] args) throws IOException, CsvReaderException {
		final long numRows;
		IHolyCube holyCube;
		Path inputFolder = Paths.get("/Users/blacelle/Downloads/csv_age_sex");

		File file = new File(inputFolder.toFile(), "Data8317.csv");

		LOGGER.info("About to parse a CSV file with length={}", PepperLogHelper.humanBytes(file.length()));
		try (final InputStream inputStream = new FileInputStream(file)) {
			final CsvSpecs specs = CsvSpecs.csv();
			final CsvReader.Result csvResult = CsvReader.read(specs, inputStream, SinkFactory.arrays());
			numRows = csvResult.numRows();
			LOGGER.info("Done parsing {} rows from a CSV file with length={}",
					PepperLogHelper.humanBytes(numRows),
					PepperLogHelper.humanBytes(file.length()));

			List<String> axes = Stream.of(csvResult.columns()).map(rc -> rc.name()).collect(Collectors.toList());
			IHolyMeasuresDefinition measures = defineMeasures(csvResult.columns());
			LOGGER.info("Measures: {}", measures);

			Set<String> measuredAxes =
					measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

			HolyCubeSink sink = new HolyCubeSink(measures);

			{

				IHolyRecordsTable cellsTable =
						new CsvHolyRecordsTable(axes, numRows, csvResult, Predicates.not(measuredAxes::contains));
				IHolyRecordsTable measuresTable =
						new CsvHolyRecordsTable(axes, numRows, csvResult, measuredAxes::contains);

				sink.sink(cellsTable, measuresTable);
			}

			holyCube = sink.closeToHolyCube();
		}
		LOGGER.info("We have an immutable cube ready for querying");

		long sizeInBytes = holyCube.getSizeInBytes();
		long deepSize = PepperFootprintHelper.deepSize(holyCube);
		LOGGER.info("CSV.length={} is represented by holyCube.length={} holyCube.deepSize={}",
				PepperLogHelper.humanBytes(file.length()),
				PepperLogHelper.humanBytes(sizeInBytes),
				PepperLogHelper.humanBytes(deepSize));

		sanityChecks(holyCube, numRows);
	}

	private static void sanityChecks(IHolyCube holyCube, long numRows) {
		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregateHelper.singleMeasureToNavigableMap(holyCube, countRecords);
			LOGGER.info("Total records: {}", result);

			Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(numRows);
		}

		{
			String wildcard = "VendorID";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(holyCube,
					AggregateQueryBuilder.edit(countRecords).addWildcard(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);
		}
	}

	public static IHolyMeasuresDefinition defineMeasures(ResultColumn[] resultColumns) {
		List<IMeasuredAxis> measuredAxes = Stream.of(resultColumns)
				.filter(cd -> DataType.FLOAT == cd.dataType() || DataType.DOUBLE == cd.dataType()
						|| cd.name().equals("count"))
				.map(cd -> new MeasuredAxis(cd.name(), IStandardOperators.SUM))
				.collect(Collectors.toCollection(ArrayList::new));

		Assertions.assertThat(measuredAxes).hasSize(1).contains(new MeasuredAxis("count", IStandardOperators.SUM));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURED_AXIS);

		IHolyMeasuresDefinition measures = new HolyMeasuresTableDefinition(measuredAxes);
		return measures;
	}

}
