package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IScannableLongMeasureColumn extends IScannableMeasureColumn {

	@Override
	@Deprecated
	default void acceptAggregates(Consumer<Object> aggregateConsumer) {
		acceptAggregates((LongConsumer) aggregateConsumer);
	}

	@Override
	@Deprecated
	default Iterator<Object> map(LongIterator rowsIterator) {
		LongIterator primitiveIterator = mapToLong(rowsIterator);
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

	void acceptAggregates(LongConsumer aggregateConsumer);

	LongIterator mapToLong(LongIterator rowsIterator);
}
