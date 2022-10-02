package eu.solven.holymolap.measures;

import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static final Logger LOGGER = LoggerFactory.getLogger(EmptyHolyMeasuresTable.class);

	@Override
	public long getSizeInBytes() {
		return 0;
	}

	@Override
	public DoubleIterator readDouble(LongIterator rowsIterator, int axisIndex) {
		return DoubleIterators.EMPTY_ITERATOR;
	}

	@Override
	public void acceptDoubles(LongIterator rowsIterator, int axisIndex, DoubleConsumer doubleConsumer) {
		LOGGER.trace("Empty");
	}

	@Override
	public IHolyMeasuresDefinition getDefinition() {
		return new EmptyHolyMeasureTableDefinition();
	}

	@Override
	public LongIterator readLong(LongIterator rowsIterator, int measureIndex) {
		return LongIterators.EMPTY_ITERATOR;
	}

	@Override
	public void acceptLongs(LongIterator rowsIterator, int axisIndex, LongConsumer longConsumer) {
		LOGGER.trace("Empty");
	}
}
