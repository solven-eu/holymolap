package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;

import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * A measure column is a read-only column of aggregates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IScannableMeasureColumn extends IHasMemoryFootprint {

	/**
	 * 
	 * @return The number of rows in given column.
	 */
	long getRows();

	Object neutral();

	void acceptAggregates(PrimitiveIterator.OfLong rowsIterator, Consumer<Object> aggregateConsumer);

	Iterator<Object> map(PrimitiveIterator.OfLong rowsIterator);

}
