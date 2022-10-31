package eu.solven.holymolap.measures;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;
import eu.solven.holymolap.immutable.column.IScannableLongMeasureColumn;
import eu.solven.holymolap.immutable.column.IScannableMeasureColumn;
import eu.solven.holymolap.measures.definition.EmptyHolyMeasureTableDefinition;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongIterators;

/**
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyMeasuresTable implements IHolyMeasuresTable {
	final IHolyMeasuresDefinition definition;

	/**
	 * For each key, gives the value as double, which would be valid only where the key-bitmap is true
	 */
	protected final List<? extends IScannableMeasureColumn> axisIndexToAggregates;

	public HolyMeasuresTable() {
		this.definition = new EmptyHolyMeasureTableDefinition();
		this.axisIndexToAggregates = ImmutableList.of();
	}

	public HolyMeasuresTable(IHolyMeasuresDefinition definition,
			List<? extends IScannableMeasureColumn> axisIndexToAggregates) {
		this.definition = definition;
		this.axisIndexToAggregates = axisIndexToAggregates;
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		sizeInBytes += axisIndexToAggregates.stream().mapToLong(IHasMemoryFootprint::getSizeInBytes).sum();

		return sizeInBytes;
	}

	@Override
	public IHolyMeasuresDefinition getMeasuresDefinition() {
		return definition;
	}

	@Override
	public DoubleIterator readDouble(final LongIterator rowsIterator, final int measureIndex) {
		if (measureIndex < 0) {
			return DoubleIterators.EMPTY_ITERATOR;
		} else if (measureIndex >= axisIndexToAggregates.size()) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		IScannableDoubleMeasureColumn doubles = (IScannableDoubleMeasureColumn) axisIndexToAggregates.get(measureIndex);
		if (doubles == null) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		return doubles.mapToDouble(rowsIterator);
	}

	@Override
	public LongIterator readLong(LongIterator rowsIterator, int measureIndex) {
		if (measureIndex == IHolyMeasuresDefinition.CELLCOUNT_MEASURE_INDEX) {
			// When counting, we need to consider any input cell
			return new LongIterator() {

				@Override
				public boolean hasNext() {
					return rowsIterator.hasNext();
				}

				@Override
				public long nextLong() {
					// Each cell is considered as contributing 1 cell.
					return 1L;
				}
			};
		} else if (measureIndex < 0) {
			return LongIterators.EMPTY_ITERATOR;
		} else if (measureIndex >= axisIndexToAggregates.size()) {
			return LongIterators.EMPTY_ITERATOR;
		}

		IScannableLongMeasureColumn longs = (IScannableLongMeasureColumn) axisIndexToAggregates.get(measureIndex);
		if (longs == null) {
			return LongIterators.EMPTY_ITERATOR;
		}

		return longs.mapToLong(rowsIterator);
	}
}
