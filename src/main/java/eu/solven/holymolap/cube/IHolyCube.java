package eu.solven.holymolap.cube;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.index.IHolyCubeIndex;
import eu.solven.holymolap.query.IAggregationLogic;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

/**
 * An {@link IHolyCube} is the multi-dimentional data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCube extends IHasMemoryFootprint {
	RoaringBitmap getFiltersBitmap(IHasFilters hasFilters);

	IHolyCubeIndex getIndex();

	long getNbRows();

	/**
	 * Used by {@link IAggregationLogic}
	 * 
	 * @param rowIterator
	 * @param doubleAxisIndex
	 * @param defaultValue
	 * @return
	 */
	DoubleIterator readDouble(IntIterator rowIterator, int doubleAxisIndex, double defaultValue);
}
