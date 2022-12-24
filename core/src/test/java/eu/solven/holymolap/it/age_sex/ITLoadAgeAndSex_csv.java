package eu.solven.holymolap.it.age_sex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.roaringbitmap.IntConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.immutable.table.IHolyDictionarizedTable;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.primitives.HolyPrimitiveParser;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.AggregationToMapHelper;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.query.SimpleAggregationQuery;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.csv.LoadFromCsv;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTableVisitor;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import eu.solven.pepper.memory.PepperFootprintHelper;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.reading.CsvReader.ResultColumn;
import io.deephaven.csv.util.CsvReaderException;

public class ITLoadAgeAndSex_csv {

	private static final Logger LOGGER = LoggerFactory.getLogger(ITLoadAgeAndSex_csv.class);

	// TODO Enable passenger_count as axes
	// TODO Enable year(tpep_pickup_datetime) and year(tpep_dropoff_datetime)
	public static void main(String[] args) throws IOException, CsvReaderException {
		Path inputFolder = Paths.get("/Users/blacelle/Downloads/csv_age_sex");

		File file = new File(inputFolder.toFile(), "Data8317.csv");

		LoadResult loadResult;

		loadResult = new LoadFromCsv(new LoadingContext("csv_age_sex")) {

			@Override
			protected IHolyRecordsTable adjustMeasures(IHolyRecordsTable measuresTable) {
				return ITLoadAgeAndSex_csv.cleanMeasures(measuresTable);
			}

			@Override
			protected IHolyMeasuresDefinition defineMeasures(ResultColumn[] resultColumns) {
				return ITLoadAgeAndSex_csv.defineMeasures(resultColumns);
			}

			protected long limitNumRows(long numRows) {
				// return Math.min(numRows, 50);
				return numRows;

			};
		}.loadSingleCsvFile(file);

		IHolyCube holyCube = loadResult.getHolyCube();
		{
			long sizeInBytes = holyCube.getSizeInBytes();
			long deepSize = PepperFootprintHelper.deepSize(holyCube);
			LOGGER.info("(Before .trim()) CSV.length={} is represented by holyCube.length={} holyCube.deepSize={}",
					PepperLogHelper.humanBytes(file.length()),
					PepperLogHelper.humanBytes(sizeInBytes),
					PepperLogHelper.humanBytes(deepSize));
		}
		sanityChecks(holyCube, loadResult.getNumRows());

		holyCube.trim();
		// Invalidate cache after trim, as some cache may help the trim
		holyCube.invalidateCache();
		{
			long sizeInBytes = holyCube.getSizeInBytes();
			long deepSize = PepperFootprintHelper.deepSize(holyCube);
			LOGGER.info("(After .trim()) CSV.length={} is represented by holyCube.length={} holyCube.deepSize={}",
					PepperLogHelper.humanBytes(file.length()),
					PepperLogHelper.humanBytes(sizeInBytes),
					PepperLogHelper.humanBytes(deepSize));
		}
		sanityChecks(holyCube, loadResult.getNumRows());

		// holyCube.reduceBy()

		// generateCellSet(holyCube.getCellSet().getAxesWithCoordinates().axes().size(),
		// holyCube.getCellSet().getTable());
	}

	/**
	 * Used to write as file. Useful to benchmark cellSet compression schemes.
	 * 
	 * @param nbAxes
	 * @param table
	 * @throws IOException
	 */
	private static void generateCellSet(int nbAxes, IHolyDictionarizedTable table) throws IOException {
		int[] axesIndexes = IntStream.range(0, nbAxes).toArray();

		Path tmpFile = Files.createTempFile("holymolap-cellset-csv_age_sex-", ".txt");

		try (BufferedWriter os = new BufferedWriter(new FileWriter(tmpFile.toFile()))) {
			table.getAll().forEach((IntConsumer) i -> {
				long[] coordinates = table.getCellCoordinates(i, axesIndexes);

				String asString =
						LongStream.of(coordinates).mapToObj(l -> Long.toString(l)).collect(Collectors.joining(","));
				try {
					os.write(asString);
					os.write("\r\n");
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		}
		LOGGER.info("We wrote cells at: {}", tmpFile);
	}

	// Demonstrate how one can clean data between a source and a HolyCubeSink
	private static IHolyRecordsTable cleanMeasures(IHolyRecordsTable measuresTable) {
		return new IHolyRecordsTable() {

			@Override
			public long size() {
				return measuresTable.size();
			}

			@Override
			public List<String> getAxes() {
				return measuresTable.getAxes();
			}

			@Override
			public void accept(IHolyRecordsTableVisitor visitor) {
				measuresTable.accept(new IHolyRecordsTableVisitor() {

					@Override
					public void onObject(int axisIndex, List<?> listObjects) {
						if (getAxes().get(axisIndex).equals("count")) {
							// 'count' is ill in the CSV: we prefer cleaning it early
							double[] cleanDoubles = listObjects.stream().mapToDouble(o -> {
								if ("..C".equals(o)) {
									// There is a lot of such Strings in the CSV: it is faster to discard them
									// early than trying to parse as a double
									return Double.NaN;
								} else {
									return HolyPrimitiveParser.toDouble(o);
								}
							}).toArray();

							visitor.onDouble(axisIndex, cleanDoubles);
						} else {
							visitor.onObject(axisIndex, listObjects);
						}
					}
				});
			}
		};
	}

	public static IHolyMeasuresDefinition defineMeasures(ResultColumn[] resultColumns) {
		List<IMeasuredAxis> measuredAxes = Stream.of(resultColumns)
				.filter(cd -> DataType.FLOAT == cd.dataType() || DataType.DOUBLE == cd.dataType()
						|| cd.name().equals("count"))
				.flatMap(cd -> Stream.of(new MeasuredAxis(cd.name(), IStandardOperators.SUM),
						new MeasuredAxis(cd.name(), IStandardOperators.SAFE_SUM)))
				.collect(Collectors.toCollection(ArrayList::new));

		Assertions.assertThat(measuredAxes)
				.hasSize(2)
				.contains(new MeasuredAxis("count", IStandardOperators.SUM))
				.contains(new MeasuredAxis("count", IStandardOperators.SAFE_SUM));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURE);

		IHolyMeasuresDefinition measures = new HolyMeasuresTableDefinition(measuredAxes);
		return measures;
	}

	private static void sanityChecks(IHolyCube holyCube, long numRows) {
		SimpleAggregationQuery countRecords = AggregateQueryBuilder.grandTotal().count("*").build();

		{
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(holyCube, countRecords);
			LOGGER.info("Total records: {}", result);

			Assertions.assertThat((long) result.values().iterator().next()).isEqualTo(numRows);
		}

		{
			String wildcard = "Sex";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(holyCube,
							AggregateQueryBuilder.edit(countRecords).addWildcards(wildcard).build());
			LOGGER.info("Total records by '{}': {}", wildcard, result);
		}

		SimpleAggregationQuery sumCounts = AggregateQueryBuilder.grandTotal().sum("count").build();
		{
			String wildcard = "Sex";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(holyCube,
							AggregateQueryBuilder.edit(sumCounts).addWildcards(wildcard).build());
			LOGGER.info("SUM(count) by '{}': {}", wildcard, result);
		}

		SimpleAggregationQuery safeSumCounts =
				AggregateQueryBuilder.grandTotal().addAggregations(OperatorFactory.safeSum("count")).build();
		{
			String wildcard = "Sex";
			NavigableMap<? extends NavigableMap<?, ?>, ?> result =
					AggregationToMapHelper.singleMeasureToNavigableMap(holyCube,
							AggregateQueryBuilder.edit(safeSumCounts).addWildcards(wildcard).build());
			LOGGER.info("SUM(count) by '{}': {}", wildcard, result);
		}
	}

}
