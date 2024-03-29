package eu.solven.holymolap.sink.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;
import io.deephaven.csv.CsvSpecs;
import io.deephaven.csv.parsers.DataType;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.reading.CsvReader.ResultColumn;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.util.CsvReaderException;

/**
 * Demonstrate how to load a CSV file into an {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public class LoadFromCsv {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadFromCsv.class);
	final LoadingContext loadingContext;

	public LoadFromCsv() {
		this(new LoadingContext());
	}

	public LoadFromCsv(LoadingContext loadingContext) {
		this.loadingContext = loadingContext;
	}

	public LoadResult loadSingleCsvFile(File file) throws CsvReaderException, IOException, FileNotFoundException {
		LoadResult loadResult;
		LOGGER.info("About to parse a CSV file with length={}", PepperLogHelper.humanBytes(file.length()));
		try (final InputStream inputStream = new FileInputStream(file)) {
			final long numRows;
			IHolyCube holyCube;

			final CsvSpecs specs = CsvSpecs.csv();
			final CsvReader.Result csvResult = CsvReader.read(specs, inputStream, SinkFactory.arrays());
			numRows = csvResult.numRows();
			LOGGER.info("Done parsing {} rows from a CSV file with length={}",
					PepperLogHelper.humanBytes(numRows),
					PepperLogHelper.humanBytes(file.length()));

			Stream.of(csvResult.columns())
					.forEach(rc -> LOGGER.info("Column: {}->{} ({})", rc.name(), rc.dataType(), rc.data().getClass()));

			List<String> axes = Stream.of(csvResult.columns()).map(rc -> rc.name()).collect(Collectors.toList());
			IHolyMeasuresDefinition measures = defineMeasures(csvResult.columns());
			LOGGER.info("Measures: {}", measures);

			Set<String> measuredAxes =
					measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

			HolyCubeSink sink = new HolyCubeSink(loadingContext, measures);

			long limittedNumRows = limitNumRows(numRows);

			{

				IHolyRecordsTable cellsTable = defaultCells(limittedNumRows, csvResult, axes, measuredAxes);
				IHolyRecordsTable defaultMeasuresTable =
						defaultMeasures(limittedNumRows, csvResult, axes, measuredAxes);

				IHolyRecordsTable adjustedMeasuresTable = adjustMeasures(defaultMeasuresTable);

				sink.sink(cellsTable, new DenormalizeHolyMeasuresRecordsTable(adjustedMeasuresTable, measures));
			}

			holyCube = sink.closeToHolyCube();

			loadResult = new LoadResult(limittedNumRows, holyCube);
		}
		LOGGER.info("We have an immutable cube ready for querying");
		return loadResult;
	}

	protected long limitNumRows(long numRows) {
		return numRows;
	}

	protected CsvHolyRecordsTable defaultCells(final long numRows,
			final CsvReader.Result csvResult,
			List<String> axes,
			Set<String> measuredAxes) {
		return new CsvHolyRecordsTable(axes, numRows, csvResult, Predicates.not(measuredAxes::contains));
	}

	protected CsvHolyRecordsTable defaultMeasures(final long numRows,
			final CsvReader.Result csvResult,
			List<String> axes,
			Set<String> measuredAxes) {
		return new CsvHolyRecordsTable(axes, numRows, csvResult, measuredAxes::contains);
	}

	/**
	 * Default behavior is to SUM any float/double column, and COUNT(*)
	 * 
	 * @param columns
	 * @return the measures to be computed.
	 */
	protected IHolyMeasuresDefinition defineMeasures(ResultColumn[] columns) {
		List<IMeasuredAxis> measuredAxes = Stream.of(columns)
				.filter(cd -> DataType.FLOAT == cd.dataType() || DataType.DOUBLE == cd.dataType())
				.map(cd -> new MeasuredAxis(cd.name(), IStandardOperators.SUM))
				.collect(Collectors.toCollection(ArrayList::new));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURE);

		return new HolyMeasuresTableDefinition(measuredAxes);
	}

	protected IHolyRecordsTable adjustMeasures(IHolyRecordsTable measuresTable) {
		return measuresTable;
	}
}
