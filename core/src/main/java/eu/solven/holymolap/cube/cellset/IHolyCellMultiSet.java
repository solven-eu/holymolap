package eu.solven.holymolap.cube.cellset;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;
import eu.solven.holymolap.immutable.table.IHolyDictionarizedTable;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * {@link IHolyCellMultiSet} dedicates to storing the cells (i.e. the coordinates of a cube, not its aggregates). It
 * should be seen as an injection from a long to a fine-grain cells (expressing as many axes as possible). 2 equivalent
 * cells may be mapped to different cellIndexes.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyCellMultiSet extends IHasMemoryFootprint {

	RoaringBitmap EMPTY_BITMAP = HolyExceptionManagement.immutableEmptyBitmap();

	IHasAxesWithCoordinates getAxesWithCoordinates();

	IHolyDictionarizedTable getTable();

}
