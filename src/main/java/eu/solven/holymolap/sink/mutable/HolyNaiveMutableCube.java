package eu.solven.holymolap.sink.mutable;

import java.util.stream.Stream;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.sink.IFastEntry;
import eu.solven.holymolap.sink.IHolySink;
import eu.solven.holymolap.sink.ISinkContext;

/**
 * This knows how to accumulate rows from an {@link IHolySink} to later produce an {@link IHolyCube}.
 * 
 * This one is naive, as it does not apply any compression.
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyNaiveMutableCube implements IHolySink {

	@Override
	public IHolyCube sink(ISinkContext context, Stream<? extends IFastEntry> toAdd) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
