package eu.solven.holymolap.immutable.column;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public interface IScannableDoubleMeasureColumn extends IScannableMeasureColumn {
	@Override
	@Deprecated
	default void acceptAggregates(Consumer<Object> aggregateConsumer) {
		acceptAggregates((DoubleConsumer) aggregateConsumer);
	}

	@Override
	@Deprecated
	default Iterator<Object> map(LongIterator rowsIterator) {
		DoubleIterator primitiveIterator = mapToDouble(rowsIterator);
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

	void acceptAggregates(DoubleConsumer aggregateConsumer);

	DoubleIterator mapToDouble(LongIterator rowsIterator);
}
