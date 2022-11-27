package eu.solven.holymolap.measures;

import java.util.PrimitiveIterator;

import eu.solven.holymolap.measures.definition.EmptyHolyMeasureTableDefinition;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongIterators;

/**
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
		return new EmptyHolyMeasureTableDefinition();
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
