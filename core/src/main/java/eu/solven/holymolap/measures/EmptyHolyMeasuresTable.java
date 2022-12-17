package eu.solven.holymolap.measures;

import java.util.Arrays;
import java.util.PrimitiveIterator;

import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.query.ICountMeasuresConstants;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongIterators;

/**
 * A minimal {@link IHolyMeasuresTable}. It can answer COUNT(*).
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyHolyMeasuresTable implements IHolyMeasuresTable {
	@Override
	public long getSizeInBytes() {
		return 0;
	}

	@Override
	public IHolyMeasuresDefinition getMeasuresDefinition() {
		return new HolyMeasuresTableDefinition(Arrays.asList(ICountMeasuresConstants.COUNT_MEASURE));
	}

	@Override
	public DoubleIterator readDouble(PrimitiveIterator.OfLong rowsIterator, int axisIndex) {
		return DoubleIterators.EMPTY_ITERATOR;
	}

	@Override
	public LongIterator readLong(PrimitiveIterator.OfLong rowsIterator, int measureIndex) {
		return LongIterators.EMPTY_ITERATOR;
	}
}
