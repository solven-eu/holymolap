package eu.solven.holymolap.cube.aggregates;

import java.util.List;
import java.util.function.DoubleConsumer;

import eu.solven.holymolap.cube.immutable.IScannableDoubleAggregatesColumn;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyAggregateTable implements IHolyAggregateTable {
	final IHolyAggregateTableDefinition definition;

	/**
	 * For each key, gives the value as double, which would be valid only where the key-bitmap is true
	 */
	protected final List<? extends IScannableDoubleAggregatesColumn> axisIndexToDoubles;

	public HolyAggregateTable(IHolyAggregateTableDefinition definition,
			List<? extends IScannableDoubleAggregatesColumn> axisIndexToDoubles) {
		this.definition = definition;
		this.axisIndexToDoubles = axisIndexToDoubles;
	}

	@Override
	public DoubleIterator readDouble(final LongIterator rowsIterator, final int axisIndex) {
		if (axisIndex < 0) {
			return DoubleIterators.EMPTY_ITERATOR;
		} else if (axisIndex >= axisIndexToDoubles.size()) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		IScannableDoubleAggregatesColumn doubles = axisIndexToDoubles.get(axisIndex);
		if (doubles == null) {
			return DoubleIterators.EMPTY_ITERATOR;
		}

		return doubles.mapToDouble(rowsIterator);
	}

	@Override
	public long getSizeInBytes() {
		long sizeInBytes = 0;

		sizeInBytes += axisIndexToDoubles.stream().mapToLong(IHasMemoryFootprint::getSizeInBytes).sum();

		return sizeInBytes;
	}

	@Override
	public void acceptDoubles(LongIterator rowsIterator, int axisIndex, DoubleConsumer doubleConsumer) {
		readDouble(rowsIterator, axisIndex).forEachRemaining(doubleConsumer);
	}

	@Override
	public IHolyAggregateTableDefinition getDefinition() {
		return definition;
	}
}
