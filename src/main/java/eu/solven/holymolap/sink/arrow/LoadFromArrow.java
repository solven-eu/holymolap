package eu.solven.holymolap.sink.arrow;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.IStandardOperators;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

/**
 * Demonstrate how to load a CSV file into an {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public class LoadFromArrow {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadFromArrow.class);

	final LoadingContext loadingContext;

	public LoadFromArrow() {
		this(new LoadingContext());
	}

	public LoadFromArrow(LoadingContext loadingContext) {
		this.loadingContext = loadingContext;
	}

	public LoadResult loadParquetFile(URI uri) {
		// LOGGER.info("About to parse a PARQUET file with length={}", PepperLogHelper.humanBytes(file.length()));

		// https://arrow.apache.org/cookbook/java/io.html#reading-parquet-file
		ScanOptions options = new ScanOptions(32768 * 1024);
		try (BufferAllocator allocator = new RootAllocator();
				DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator,
						NativeMemoryPool.getDefault(),
						inferFileFormat(uri),
						uri.toString());
				Dataset dataset = datasetFactory.finish();
				Scanner scanner = dataset.newScan(options);
				ArrowReader reader = scanner.scanBatches()) {

			HolyCubeSink sink = null;

			long recordsCount = 0L;

			while (reader.loadNextBatch()) {
				try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
					Schema arrowSchema = root.getSchema();

					if (sink == null) {
						IHolyMeasuresDefinition measures = defineMeasures(arrowSchema);
						sink = new HolyCubeSink(loadingContext, measures);
					}

					List<String> axes = new ArrayList<>(arrowSchema.getFields().size());
					for (Field field : arrowSchema.getFields()) {
						axes.add(field.getName());
					}

					int rowCount = root.getRowCount();

					recordsCount += rowCount;

					List<FieldVector> fieldVectors = root.getFieldVectors();

					// By default, we consider as cellAxes only if not a measure column
					IHolyRecordsTable cellsToAdd =
							new ArrowHolyRecordsTable(axes, rowCount, fieldVectors, axes::contains);
					IHolyRecordsTable measuresToAdd =
							new ArrowHolyRecordsTable(axes, rowCount, fieldVectors, Predicates.not(axes::contains));

					sink.sink(cellsToAdd, measuresToAdd);
				}
			}

			IHolyCube holyCube = sink.closeToHolyCube();
			LOGGER.info("We have an immutable cube ready for querying from {}", uri);
			return new LoadResult(recordsCount, holyCube);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private FileFormat inferFileFormat(URI uri) {
		if (uri.toASCIIString().toLowerCase(Locale.US).endsWith(".parquet")) {
			return FileFormat.PARQUET;
		} else {
			throw new IllegalArgumentException("Can not infer fileFormat from: " + uri);
		}
	}

	/**
	 * Default behavior is to SUM any float/double column, and COUNT(*)
	 * 
	 * @param columns
	 * @return the measures to be computed.
	 */
	protected IHolyMeasuresDefinition defineMeasures(Schema schema) {
		List<IMeasuredAxis> measuredAxes = schema.getFields()
				.stream()
				.filter(cd -> ArrowTypeID.FloatingPoint == cd.getType().getTypeID())
				.map(cd -> new MeasuredAxis(cd.getName(), IStandardOperators.SUM))
				.collect(Collectors.toCollection(ArrayList::new));

		// Enable querying COUNT(*)
		measuredAxes.add(ICountMeasuresConstants.COUNT_MEASURED_AXIS);

		return new HolyMeasuresTableDefinition(measuredAxes);
	}

	protected IHolyRecordsTable cleanMeasures(IHolyRecordsTable measuresTable) {
		return measuresTable;
	}
}
