package eu.solven.holymolap.cube;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.aggregates.IHolyAggregateTable;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * An {@link IHolyCube} is the multi-dimensional data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCube extends IHasMemoryFootprint {

	long getNbRows();

	IHolyCellMultiSet getCellSet();

	IHolyAggregateTable getAggregateTable();

	/**
	 * 
	 * @param hasFilters
	 * @return the bitmap of the cells matching given {@link IHasFilters}
	 */
	RoaringBitmap getFiltersBitmap(IHasFilters hasFilters);
}
