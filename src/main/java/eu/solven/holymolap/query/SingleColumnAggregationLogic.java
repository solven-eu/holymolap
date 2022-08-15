package eu.solven.holymolap.query;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class SingleColumnAggregationLogic implements IAggregationLogic<Double> {
	protected final String aggregatedAxis;
	protected final IDoubleBinaryOperator operator;

	public SingleColumnAggregationLogic(String aggregatedAxis, IDoubleBinaryOperator operator) {
		this.aggregatedAxis = aggregatedAxis;
		this.operator = operator;
	}

	@Override
	public Double aggregateTo(IHolyCube cube, LongIterator rowsIterator, long[] axisIndexToCoordinateIndex) {
		double neutral = operator.neutral();

		// The following is discarded due to early optimization
		// return alternativeAggregateTo(roaringCube, rowsIterator, neutral);

		DoubleIterator doubleIt =
				cube.getAggregateTable().readDouble(rowsIterator, cube.getCellSet().getAxisIndex(aggregatedAxis));

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
	protected double alternativeAggregateTo(IHolyCube cube, LongIterator rowsIterator, double neutral) {
		AtomicDouble doubleRef = new AtomicDouble(neutral);

		cube.getAggregateTable().acceptDoubles(rowsIterator, cube.getCellSet().getAxisIndex(aggregatedAxis), next -> {
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
