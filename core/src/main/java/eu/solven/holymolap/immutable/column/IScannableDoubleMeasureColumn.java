package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.stream.LongStream;

/**
 * A measure column is a read-only column of double aggregates.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IScannableDoubleMeasureColumn extends IScannableMeasureColumn {
	@Override
	@Deprecated
	default void acceptAggregates(PrimitiveIterator.OfLong rowsIterator, Consumer<Object> aggregateConsumer) {
		acceptAggregates(rowsIterator, (DoubleConsumer) aggregateConsumer);
	}

	@Override
	@Deprecated
	default Iterator<Object> map(PrimitiveIterator.OfLong rowsIterator) {
		PrimitiveIterator.OfDouble primitiveIterator = mapToDouble(rowsIterator);

		return new Iterator<Object>() {

			@Override
			public boolean hasNext() {
				return primitiveIterator.hasNext();
			}

			@Override
			public Object next() {
				return primitiveIterator.nextDouble();
			}
		};
	}

	@Deprecated
	default void acceptAggregates(DoubleConsumer aggregateConsumer) {
		acceptAggregates(LongStream.range(0, getRows()).iterator(), aggregateConsumer);
	}

	@Deprecated
	@Override
	default Object neutral() {
		return neutralAsDouble();
	}

	double neutralAsDouble();

	void acceptAggregates(PrimitiveIterator.OfLong rowsIterator, DoubleConsumer aggregateConsumer);

	PrimitiveIterator.OfDouble mapToDouble(PrimitiveIterator.OfLong rowsIterator);
}
