package eu.solven.holymolap.cube;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.cube.cellset.HolyBitmapCellMultiSet;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.exception.HolyExceptionManagement;
import eu.solven.holymolap.immutable.axes.EmptyAxisWithCoordinates;
import eu.solven.holymolap.immutable.table.EmptyHolyDictionarizedTable;
import eu.solven.holymolap.measures.EmptyHolyMeasuresTable;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.primitives.ICompactable;
import eu.solven.holymolap.stable.v1.IAxesFilter;
import eu.solven.holymolap.stable.v1.IHasFilters;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAnd;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterAxisEquals;
import eu.solven.holymolap.stable.v1.filters.IAxesFilterOr;

/**
 * The default implementation of an {@link IHolyCube}. It relies on an {@link IHolyCellMultiSet} describing the cells,
 * and an {@link IHolyMeasuresTable} describing the aggregates.
 * 
 * @author Benoit Lacelle
 *
 */
@ManagedResource
public class HolyCube implements IHolyCube {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCube.class);

	protected final int nbCells;

	protected final IHolyCellMultiSet cellSet;
	protected final IHolyMeasuresTable aggregateTable;

	public HolyCube(int nbCells, IHolyCellMultiSet cellSet, IHolyMeasuresTable aggregateTable) {
		this.nbCells = nbCells;
		this.cellSet = cellSet;
		this.aggregateTable = aggregateTable;
	}

	/**
	 * Make an empty cube
	 */
	public HolyCube() {
		this(0,
				new HolyBitmapCellMultiSet(new EmptyAxisWithCoordinates(), new EmptyHolyDictionarizedTable()),
				new EmptyHolyMeasuresTable());
	}

	@Override
	public String toString() {
		return "#cells: " + nbCells
				+ ", Axes="
				+ cellSet.getAxesWithCoordinates().axes()
				+ ", Measures="
				+ aggregateTable.getMeasuresDefinition();
	}

	@Override
	public void invalidateCache() {
		if (cellSet instanceof IMayCache) {
			((IMayCache) cellSet).invalidateCache();
		}
		if (aggregateTable instanceof IMayCache) {
			((IMayCache) aggregateTable).invalidateCache();
		}
	}

	@Override
	public void trim() {
		if (cellSet instanceof ICompactable) {
			((ICompactable) cellSet).trim();
		}
		if (aggregateTable instanceof ICompactable) {
			((ICompactable) aggregateTable).trim();
		}
	}

	@Override
	public RoaringBitmap getFiltersBitmap(IHasFilters hasFilters) {
		IAxesFilter axesFilter = hasFilters.getFilters();

		// long currentNbRows = getNbRows();

		RoaringBitmap all = cellSet.getTable().getAll();
		if (axesFilter.isMatchAll()) {
			if (axesFilter.isExclusion()) {
				// Empty
				return HolyExceptionManagement.immutableEmptyBitmap();
			} else {
				return all;
			}
		} else if (axesFilter.isAxisEquals()) {
			IAxesFilterAxisEquals equalsFilter = (IAxesFilterAxisEquals) axesFilter;

			String axis = equalsFilter.getAxis();
			int axisIndex = cellSet.getAxesWithCoordinates().getAxisIndex(axis);
			if (axisIndex < 0) {
				return HolyExceptionManagement.immutableEmptyBitmap();
			}
			long coordinateRef = cellSet.getAxesWithCoordinates().getCoordinateRef(axis, equalsFilter.getFiltered());

			RoaringBitmap valueBitmap = cellSet.getTable().getCoordinateToRows(axisIndex, coordinateRef);

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, valueBitmap);
			} else {
				return valueBitmap;
			}
		} else if (axesFilter.isAnd()) {
			IAxesFilterAnd andFilter = (IAxesFilterAnd) axesFilter;

			List<RoaringBitmap> andBitmaps = new ArrayList<>();
			for (IAxesFilter andOperand : andFilter.getAnd()) {
				RoaringBitmap entryBitmap = getFiltersBitmap(() -> andOperand);

				andBitmaps.add(entryBitmap);
			}

			if (andBitmaps.isEmpty()) {
				throw new IllegalStateException("Should have been caught by .matchAll");
			}

			RoaringBitmap andBitmap = FastAggregation.and(andBitmaps.iterator());

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, andBitmap);
			} else {
				return andBitmap;
			}
		} else if (axesFilter.isOr()) {
			IAxesFilterOr andFilter = (IAxesFilterOr) axesFilter;

			List<RoaringBitmap> orBitmaps = new ArrayList<>();
			for (IAxesFilter orFilter : andFilter.getOr()) {
				RoaringBitmap entryBitmap = getFiltersBitmap(() -> orFilter);

				orBitmaps.add(entryBitmap);
			}

			RoaringBitmap orBitmap = FastAggregation.or(orBitmaps.iterator());

			if (axesFilter.isExclusion()) {
				return RoaringBitmap.andNot(all, orBitmap);
			} else {
				return orBitmap;
			}
		} else {
			throw new UnsupportedOperationException("filter: " + axesFilter);
		}
	}

	@Override
	public long getNbRows() {
		return nbCells;
	}

	@ManagedAttribute
	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		sizeInBytes += cellSet.getSizeInBytes();
		sizeInBytes += aggregateTable.getSizeInBytes();

		return sizeInBytes;
	}

	@Override
	public IHolyCellMultiSet getCellSet() {
		return cellSet;
	}

	@Override
	public IHolyMeasuresTable getMeasuresTable() {
		return aggregateTable;
	}
}
