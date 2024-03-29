package eu.solven.holymolap.measures.aggregation;

import java.util.PrimitiveIterator;

import com.google.common.util.concurrent.AtomicDouble;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class DoubleAggregationLogic implements IDoubleAggregationLogic {
	// Relate to a IHolyMeasuresDefinition
	protected final int measureIndex;
	protected final IDoubleBinaryOperator operator;

	public static IAggregationLogic<?> search(IHolyMeasuresDefinition definition, IMeasuredAxis measuredAxis) {
		int index = definition.findMeasureIndex(measuredAxis);

		if (index < 0) {
			throw new IllegalArgumentException("Can not find " + measuredAxis + " in " + definition);
		}

		return definition.measures().get(index).getAggregationLogic();
	}

	DoubleAggregationLogic() {
		// serialization
		this.measureIndex = Integer.MIN_VALUE;
		this.operator = null;
	}

	public DoubleAggregationLogic(int measureIndex, IDoubleBinaryOperator operator) {
		this.measureIndex = measureIndex;
		this.operator = operator;
	}

	@Override
	public IDoubleBinaryOperator getOperator() {
		return operator;
	}

	@Override
	public String toString() {
		return "SingleColumnAggregationLogic [measureIndex=" + measureIndex + ", operator=" + operator + "]";
	}

	@Override
	public double aggregateToDouble(IHolyMeasuresTable measuresTable,
			LongIterator rowsIterator,
			CoordinatesRefs slice) {
		double neutral = operator.neutralAsDouble();

		// The following is discarded due to early optimization
		// return alternativeAggregateTo(roaringCube, rowsIterator, neutral);

		PrimitiveIterator.OfDouble aggregateIt = measuresTable.readDouble(rowsIterator, measureIndex);

		// Initialize ourself with the aggregation neutral element
		double aggregate = neutral;

		while (aggregateIt.hasNext()) {
			double next = aggregateIt.nextDouble();

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
