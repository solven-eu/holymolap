package eu.solven.holymolap.sink.mutable;

import java.util.stream.Stream;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.index.IHolyCubeIndex;
import eu.solven.holymolap.sink.IFastEntry;
import eu.solven.holymolap.sink.IHolySink;
import eu.solven.holymolap.sink.ISinkContext;
import eu.solven.holymolap.stable.v1.IHasFilters;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

/**
 * This knows how to accumulate rows from an {@link IHolySink} to later produce an {@link IHolyCube}.
 * 
 * This one is naive, as it does not apply any compression.
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyNaiveCube implements IHolyCube {

	// Naive implementation
	@Override
	public long getSizeInBytes() {
		return Long.MAX_VALUE;
	}

	@Override
	public RoaringBitmap getFiltersBitmap(IHasFilters hasFilters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IHolyCubeIndex getIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNbRows() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DoubleIterator readDouble(IntIterator rowIterator, int doubleAxisIndex, double defaultValue) {
		// TODO Auto-generated method stub
		return null;
	}

}
