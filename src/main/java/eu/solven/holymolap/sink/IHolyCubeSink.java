package eu.solven.holymolap.sink;

import java.util.Iterator;
import java.util.stream.Stream;

import com.google.common.collect.Streams;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.sink.record.EmptyHolyRecord;
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

	@Deprecated
	default IHolyCube sink(ISinkContext context, IHolyRecord toAdd) {
		return sinkDeprecated(context, Stream.of(toAdd));
	}

	@Deprecated
	default IHolyCube sinkDeprecated(ISinkContext context, Iterator<? extends IHolyRecord> toAdd) {
		return sinkDeprecated(context, Streams.stream(toAdd));
	}

	@Deprecated
	default IHolyCube sinkDeprecated(ISinkContext context, Stream<? extends IHolyRecord> toAdd) {
		return sink(context, toAdd.map(r -> new HolyCubeRecord(r, new EmptyHolyRecord())));
	}

	default IHolyCube sink(ISinkContext context, IHolyCubeRecord... toAdd) {
		return sink(context, Stream.of(toAdd));
	}

	default IHolyCube sink(ISinkContext context, Iterable<? extends IHolyCubeRecord> toAdd) {
		return sink(context, toAdd.iterator());
	}

	default IHolyCube sink(ISinkContext context, Iterator<? extends IHolyCubeRecord> toAdd) {
		return sink(context, Streams.stream(toAdd));
	}

	IHolyCube sink(ISinkContext context, Stream<? extends IHolyCubeRecord> toAdd);
}
