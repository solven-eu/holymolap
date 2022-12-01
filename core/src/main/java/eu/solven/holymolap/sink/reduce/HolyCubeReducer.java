package eu.solven.holymolap.sink.reduce;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.LoadingContext;
import eu.solven.holymolap.sink.csv.LoadFromCsv;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.holymolap.sink.record.FilterInHolyRecord;
import eu.solven.holymolap.sink.record.HolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyMeasuresRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.pepper.logging.PepperLogHelper;

public class HolyCubeReducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadFromCsv.class);
	final LoadingContext loadingContext;

	public HolyCubeReducer() {
		this(new LoadingContext());
	}

	public HolyCubeReducer(LoadingContext loadingContext) {
		this.loadingContext = loadingContext;
	}

	/**
	 * 
	 * @param inputHolyCube
	 * @param newAxes
	 *            the new Set of axes.
	 * @return
	 */
	public LoadResult reduceHierarchies(IHolyCube inputHolyCube, Set<String> newAxes) {
		LoadResult loadResult;
		long numRows = inputHolyCube.getNbRows();
		LOGGER.info("About to reduce a cube with nbRows={}", PepperLogHelper.humanBytes(numRows));
		{
			IHolyCube holyCube;

			List<IMeasuredAxis> measures = defineMeasures(inputHolyCube);
			NavigableMap<? extends NavigableMap<String, ?>, Map<IMeasuredAxis, ?>> result =
					AggregateHelper.measuresToNavigableMap(inputHolyCube.asComposite(),
							AggregateQueryBuilder.grandTotal().addWildcards(newAxes).addAggregations(measures).build());

			HolyCubeSink sink = new HolyCubeSink(loadingContext, new HolyMeasuresTableDefinition(measures));

			result.entrySet().forEach(cellToMeasures -> {
				IHolyRecord cellsetRecord =
						new FilterInHolyRecord(new SlowMapHolyRecord(cellToMeasures.getKey()), newAxes);
				IHolyMeasuresRecord measuresRecord = new MapHolyMeasuresRecord(cellToMeasures.getValue());
				sink.sink(new HolyCubeRecord(cellsetRecord, measuresRecord));
			});

			holyCube = sink.closeToHolyCube();

			loadResult = new LoadResult(numRows, holyCube);
		}
		LOGGER.info("We have an immutable cube ready for querying");
		return loadResult;
	}

	private List<IMeasuredAxis> defineMeasures(IHolyCube inputHolyCube) {
		return inputHolyCube.getMeasuresTable()
				.getMeasuresDefinition()
				.measures()
				.stream()
				.map(IHolyMeasureColumnMeta::asMeasuredAxis)
				.collect(Collectors.toList());
	}

	protected IHolyRecordsTable cleanMeasures(IHolyRecordsTable measuresTable) {
		return measuresTable;
	}
}
