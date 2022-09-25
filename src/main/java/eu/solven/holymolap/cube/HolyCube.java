package eu.solven.holymolap.cube;

import java.util.ArrayList;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import eu.solven.holymolap.axes.EmptyAxisWithCoordinates;
import eu.solven.holymolap.cube.cellset.HolyBitmapCellMultiSet;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.cube.measures.EmptyHolyMeasuresTable;
import eu.solven.holymolap.cube.measures.IHolyMeasuresTable;
import eu.solven.holymolap.cube.table.EmptyHolyDictionarizedTable;
import eu.solven.holymolap.exception.HolyExceptionManagement;
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

	protected final int nbRows;

	protected final IHolyCellMultiSet cellSet;
	protected final IHolyMeasuresTable aggregateTable;

	public HolyCube(int nbRows, IHolyCellMultiSet cellSet, IHolyMeasuresTable aggregateTable) {
		this.nbRows = nbRows;
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
		return "#Rows: " + nbRows + ", Axes=" + cellSet.getAxesWithCoordinates().axes();
	}

	@Override
	public RoaringBitmap getFiltersBitmap(IHasFilters hasFilters) {
		IAxesFilter axesFilter = hasFilters.getFilters();

		long currentNbRows = getNbRows();

		RoaringBitmap all = RoaringBitmap.bitmapOfRange(0, currentNbRows);
		if (axesFilter.isMatchAll()) {
			if (axesFilter.isExclusion()) {
				// Empty
				return HolyExceptionManagement.immutableEmptyBitmap();
			} else {
				return all;
			}
		} else if (axesFilter.isAxisEquals()) {
			IAxesFilterAxisEquals equalsFilter = (IAxesFilterAxisEquals) axesFilter;

			RoaringBitmap valueBitmap =
					cellSet.getCoordinateToBitmap(equalsFilter.getAxis(), equalsFilter.getFiltered());

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

			RoaringBitmap andBitmap = RoaringBitmap.and(andBitmaps.iterator(), 0L, currentNbRows);

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

			RoaringBitmap orBitmap = RoaringBitmap.or(orBitmaps.iterator(), 0L, currentNbRows);

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
		return nbRows;
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
