package eu.solven.holymolap.sink;

import java.util.Collections;
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
	default IHolyCube sink(IFastEntry toAdd, ISinkContext context) {
		return sink(Collections.singleton(toAdd), context);
	}

	default IHolyCube sink(Iterable<? extends IFastEntry> toAdd, ISinkContext context) {
		return sink(toAdd.iterator(), context);
	}

	default IHolyCube sink(Iterator<? extends IFastEntry> toAdd, ISinkContext context) {
		return sink(Streams.stream(toAdd), context);
	}

	IHolyCube sink(Stream<? extends IFastEntry> toAdd, ISinkContext context);
}
