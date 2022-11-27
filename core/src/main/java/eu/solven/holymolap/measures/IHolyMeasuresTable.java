package eu.solven.holymolap.measures;

import java.util.PrimitiveIterator;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.measures.aggregation.IAggregationLogic;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * Holds the aggregates of an {@link IHolyCube}. It should be seen as a table with N rows, each row being attached to a
 * single cell of a {@link IHolyCellMultiSet}. Each row is attached to aggregated values (typically doubles).
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasuresTable extends IHasMemoryFootprint, IHasMeasuresDefinition {

	/**
	 * Used by {@link IAggregationLogic}
	 * 
	 * @param rowsIterator
	 * @param measureIndex
	 * @return a {@link PrimitiveIterator.OfDouble} covering the rows described by the iterator.
	 */
	PrimitiveIterator.OfDouble readDouble(PrimitiveIterator.OfLong rowsIterator, int measureIndex);

	default void acceptDoubles(PrimitiveIterator.OfLong rowsIterator, int measureIndex, DoubleConsumer doubleConsumer) {
		readDouble(rowsIterator, measureIndex).forEachRemaining(doubleConsumer);
	}

	PrimitiveIterator.OfLong readLong(PrimitiveIterator.OfLong rowsIterator, int measureIndex);

	default void acceptLongs(PrimitiveIterator.OfLong rowsIterator, int measureIndex, LongConsumer longConsumer) {
		readLong(rowsIterator, measureIndex).forEachRemaining(longConsumer);
	}

}
