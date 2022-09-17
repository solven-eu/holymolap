package eu.solven.holymolap.query;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.aggregates.IHolyMeasureTable;
import eu.solven.holymolap.cube.aggregates.IHolyMeasuresTableDefinition;
import eu.solven.holymolap.stable.v1.IAggregatedAxis;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class SingleColumnAggregationLogic implements IAggregationLogic<Double> {
	protected final int measureIndex;
	protected final IDoubleBinaryOperator operator;

	public static IAggregationLogic<?> search(IHolyMeasuresTableDefinition definition, IAggregatedAxis aggregatesAxis) {
		int index = definition.findMeasureIndex(aggregatesAxis);

		return definition.measures().get(index).getAggregationLogic();
		// IDoubleBinaryOperator operator = new OperatorFactory().getDoubleBinaryOperator(aggregatesAxis.getOperator());
		// return new SingleColumnAggregationLogic(index, operator);
	}

	public SingleColumnAggregationLogic(int measureIndex, IDoubleBinaryOperator operator) {
		this.measureIndex = measureIndex;
		this.operator = operator;
	}

	@Override
	public Double aggregateTo(IHolyMeasureTable measuresTable, LongIterator rowsIterator, long[] axisIndexToCoordinateIndex) {
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
	protected double alternativeAggregateTo(IHolyMeasureTable measuresTable, LongIterator rowsIterator, double neutral) {
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
