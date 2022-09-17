package eu.solven.holymolap.cube.immutable;

import java.util.function.DoubleConsumer;

import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IScannableDoubleMeasureColumn extends IHasMemoryFootprint{

	/**
	 * 
	 * @return The number of rows in given column.
	 */
	long getRows();

	void acceptAggregates(DoubleConsumer aggregateConsumer);

	DoubleIterator mapToDouble(LongIterator rowsIterator);
}
