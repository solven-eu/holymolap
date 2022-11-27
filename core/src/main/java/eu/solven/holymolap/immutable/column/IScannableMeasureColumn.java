package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.function.Consumer;

import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IScannableMeasureColumn extends IHasMemoryFootprint {

	/**
	 * 
	 * @return The number of rows in given column.
	 */
	long getRows();

	void acceptAggregates(Consumer<Object> aggregateConsumer);

	Iterator<Object> map(LongIterator rowsIterator);
}
