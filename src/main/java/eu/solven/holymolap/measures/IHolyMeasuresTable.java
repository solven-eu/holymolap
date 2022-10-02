package eu.solven.holymolap.measures;

import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.IHolyCellMultiSet;
import eu.solven.holymolap.measures.aggregation.IAggregationLogic;
import eu.solven.holymolap.tools.IHasMemoryFootprint;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * Holds the aggregates of an {@link IHolyCube}. It should be seen as a table with N rows, each row being attached to a
 * single cell of a {@link IHolyCellMultiSet}. Each row is attached to aggregated values (typically doubles).
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasuresTable extends IHasMemoryFootprint {

	IHolyMeasuresDefinition getDefinition();

	/**
	 * Used by {@link IAggregationLogic}
	 * 
	 * @param rowsIterator
	 * @param axisIndex
	 * @return a {@link DoubleIterator} covering the rows described by the iterator.
	 */
	DoubleIterator readDouble(LongIterator rowsIterator, int axisIndex);

	LongIterator readLong(LongIterator rowsIterator, int measureIndex);

	void acceptDoubles(LongIterator rowsIterator, int axisIndex, DoubleConsumer doubleConsumer);

	void acceptLongs(LongIterator rowsIterator, int axisIndex, LongConsumer longConsumer);

}
