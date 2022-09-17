package eu.solven.holymolap.query;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.measures.IHolyMeasuresTable;
import eu.solven.holymolap.cube.measures.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class SingleColumnAggregationLogic implements IAggregationLogic<Double> {
	protected final int measureIndex;
	protected final IDoubleBinaryOperator operator;

	public static IAggregationLogic<?> search(IHolyMeasuresTableDefinition definition, IMeasuredAxis aggregatesAxis) {
		int index = definition.findMeasureIndex(aggregatesAxis);

		return definition.measures().get(index).getAggregationLogic();
	}

	public SingleColumnAggregationLogic(int measureIndex, IDoubleBinaryOperator operator) {
		this.measureIndex = measureIndex;
		this.operator = operator;
	}

	@Override
	public Double aggregateTo(IHolyMeasuresTable measuresTable,
			LongIterator rowsIterator,
			long[] axisIndexToCoordinateIndex) {
		double neutral = operator.neutral();

		// The following is discarded due to early optimization
		// return alternativeAggregateTo(roaringCube, rowsIterator, neutral);

		DoubleIterator doubleIt = measuresTable.readDouble(rowsIterator, measureIndex);

		// Initialize ourself with the aggregation neutral element
		double aggregate = neutral;

		while (doubleIt.hasNext()) {
			double next = doubleIt.nextDouble();

			// Aggregate next value
			aggregate = operator.applyAsDouble(next, aggregate);
		}

		return aggregate;
	}

	// This would enable easily concurrent aggregation
	protected double alternativeAggregateTo(IHolyMeasuresTable measuresTable,
			LongIterator rowsIterator,
			double neutral) {
		AtomicDouble doubleRef = new AtomicDouble(neutral);

		measuresTable.acceptDoubles(rowsIterator, measureIndex, next -> {
			boolean cas = false;

			do {
				double currentAggregate = doubleRef.get();
				double newAggregate = operator.applyAsDouble(currentAggregate, next);

				cas = doubleRef.compareAndSet(currentAggregate, newAggregate);
			} while (!cas);
		});

		return doubleRef.get();
	}

}
