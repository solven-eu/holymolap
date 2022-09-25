package eu.solven.holymolap.sink;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Streams;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.cube.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.sink.record.FilterInHolyRecord;
import eu.solven.holymolap.sink.record.FilterOutHolyRecord;
import eu.solven.holymolap.sink.record.HolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyRecord;

/**
 * An {@link IHolyCubeSink} knows how to turn a set of data into a {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCubeSink {

	IHolyMeasuresDefinition getMeasures();

	@Deprecated
	default IHolyCube sink(IHolyRecord toAdd) {
		return sinkDeprecated(Stream.of(toAdd));
	}

	@Deprecated
	default IHolyCube sinkDeprecated(Iterator<? extends IHolyRecord> toAdd) {
		return sinkDeprecated(Streams.stream(toAdd));
	}

	@Deprecated
	default IHolyCube sinkDeprecated(Stream<? extends IHolyRecord> toAdd) {
		return sink(toAdd.map(r -> makeHolyCubeRecord(r)));
	}

	@Deprecated
	default IHolyCubeRecord makeHolyCubeRecord(IHolyRecord r) {
		IHolyMeasuresDefinition measures = getMeasures();
		Set<String> measuredColumns =
				measures.measures().stream().map(IHolyMeasureColumnMeta::getColumn).collect(Collectors.toSet());

		// By default, we consider as cellAxes only if not a measure column
		FilterOutHolyRecord cellRecord = new FilterOutHolyRecord(r, measuredColumns);
		FilterInHolyRecord measuresRecord = new FilterInHolyRecord(r, measuredColumns);

		return new HolyCubeRecord(cellRecord, measuresRecord);
	}

	default IHolyCube sink(IHolyCubeRecord... toAdd) {
		return sink(Stream.of(toAdd));
	}

	default IHolyCube sink(Iterable<? extends IHolyCubeRecord> toAdd) {
		return sink(toAdd.iterator());
	}

	default IHolyCube sink(Iterator<? extends IHolyCubeRecord> toAdd) {
		return sink(Streams.stream(toAdd));
	}

	IHolyCube sink(Stream<? extends IHolyCubeRecord> toAdd);
}
