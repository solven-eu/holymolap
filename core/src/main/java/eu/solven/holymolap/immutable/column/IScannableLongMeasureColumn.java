package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * A measure column is a read-only column of long aggregates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IScannableLongMeasureColumn extends IScannableMeasureColumn {

	@Override
	@Deprecated
	default void acceptAggregates(PrimitiveIterator.OfLong rowsIterator, Consumer<Object> aggregateConsumer) {
		acceptAggregates(rowsIterator, (LongConsumer) aggregateConsumer);
	}

	@Override
	@Deprecated
	default Iterator<Object> map(PrimitiveIterator.OfLong rowsIterator) {
		PrimitiveIterator.OfLong primitiveIterator = mapToLong(rowsIterator);
		return new Iterator<Object>() {

			@Override
			public boolean hasNext() {
				return primitiveIterator.hasNext();
			}

			@Override
			public Object next() {
				return primitiveIterator.nextLong();
			}
		};
	}

	@Deprecated
	@Override
	default Object neutral() {
		return neutralAsLong();
	}

	long neutralAsLong();

	void acceptAggregates(PrimitiveIterator.OfLong rowsIterator, LongConsumer aggregateConsumer);

	PrimitiveIterator.OfLong mapToLong(PrimitiveIterator.OfLong rowsIterator);
}
