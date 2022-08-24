package eu.solven.holymolap.cube.aggregates;

import java.util.function.DoubleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyHolyAggregateTable implements IHolyAggregateTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(EmptyHolyAggregateTable.class);

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
	public IHolyAggregateTableDefinition getDefinition() {
		return new EmptyHolyAggregateTableDefinition();
	}
}
