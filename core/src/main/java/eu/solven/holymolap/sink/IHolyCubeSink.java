package eu.solven.holymolap.sink;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Streams;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.mutable.cube.ICloseableToHolyCube;
import eu.solven.holymolap.sink.record.FilterInHolyRecord;
import eu.solven.holymolap.sink.record.FilterOutHolyRecord;
import eu.solven.holymolap.sink.record.HolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;

/**
 * An {@link IHolyCubeSink} knows how to turn a set of data into a {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCubeSink extends ICloseableToHolyCube {

	IHolyMeasuresDefinition getMeasures();

	@Deprecated
	default IHolyCubeSink sink(IHolyRecord toAdd) {
		return sinkDeprecated(Stream.of(toAdd));
	}

	@Deprecated
	default IHolyCubeSink sinkDeprecated(Iterator<? extends IHolyRecord> toAdd) {
		return sinkDeprecated(Streams.stream(toAdd));
	}

	@Deprecated
	default IHolyCubeSink sinkDeprecated(Stream<? extends IHolyRecord> toAdd) {
		return sink(toAdd.map(r -> makeHolyCubeRecord(r)));
	}

	@Deprecated
	default IHolyCubeRecord makeHolyCubeRecord(IHolyRecord r) {
		IHolyMeasuresDefinition measures = getMeasures();
		Set<String> measuredAxes =
				measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

		// By default, we consider as cellAxes only if not a measure column
		FilterOutHolyRecord cellRecord = new FilterOutHolyRecord(r, measuredAxes);
		FilterInHolyRecord measuresRecord = new FilterInHolyRecord(r, measuredAxes);

		return new HolyCubeRecord(cellRecord, measuresRecord);
	}

	default IHolyCubeSink sink(IHolyCubeRecord... toAdd) {
		return sink(Stream.of(toAdd));
	}

	default IHolyCubeSink sink(Iterable<? extends IHolyCubeRecord> toAdd) {
		return sink(toAdd.iterator());
	}

	default IHolyCubeSink sink(Iterator<? extends IHolyCubeRecord> toAdd) {
		return sink(Streams.stream(toAdd));
	}

	IHolyCubeSink sink(Stream<? extends IHolyCubeRecord> toAdd);

	IHolyCubeSink sink(IHolyRecordsTable cellsToAdd, IHolyRecordsTable measuresToAdd);

}
