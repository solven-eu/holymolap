package eu.solven.holymolap.cube;

import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * An {@link IHolyCube} is the multi-dimensional data-structure.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCube extends IHasMemoryFootprint, IMayCache {

	@Deprecated
	default long getNbRows() {
		return getCellSet().getTable().getAll().getLongCardinality();
	}

	IHolyCellMultiSet getCellSet();

	IHolyMeasuresTable getMeasuresTable();

	/**
	 * 
	 * @param hasFilters
	 * @return the bitmap of the cells matching given {@link IHasFilters}
	 */
	RoaringBitmap getFiltersBitmap(IHasFilters hasFilters);

	default ICompositeHolyCube asComposite() {
		return new CompositeHolyCube(Map.of("single", this));
	}
}
