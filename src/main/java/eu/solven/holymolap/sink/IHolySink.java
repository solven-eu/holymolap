package eu.solven.holymolap.sink;

import java.util.Iterator;
import java.util.stream.Stream;

import com.google.common.collect.Streams;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * An {@link IHolySink} knows how to turn a set of data into a {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolySink {
	default IHolyCube sink(ISinkContext context, IFastEntry... toAdd) {
		return sink(context, Stream.of(toAdd));
	}

	default IHolyCube sink(ISinkContext context, Iterable<? extends IFastEntry> toAdd) {
		return sink(context, toAdd.iterator());
	}

	default IHolyCube sink(ISinkContext context, Iterator<? extends IFastEntry> toAdd) {
		return sink(context, Streams.stream(toAdd));
	}

	IHolyCube sink(ISinkContext context, Stream<? extends IFastEntry> toAdd);
}
