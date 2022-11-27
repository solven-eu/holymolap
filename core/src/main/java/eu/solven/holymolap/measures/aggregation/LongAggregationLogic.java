package eu.solven.holymolap.measures.aggregation;

import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.ILongBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class LongAggregationLogic implements ILongAggregationLogic {
	protected final int measureIndex;
	protected final ILongBinaryOperator operator;

	public static IAggregationLogic<?> search(IHolyMeasuresDefinition definition, IMeasuredAxis measuredAxis) {
		int index = definition.findMeasureIndex(measuredAxis);

		if (index < 0) {
			throw new IllegalArgumentException("Can not find " + measuredAxis + " in " + definition);
		}

		return definition.measures().get(index).getAggregationLogic();
	}

	public LongAggregationLogic(int measureIndex, ILongBinaryOperator operator) {
		this.measureIndex = measureIndex;
		this.operator = operator;
	}

	@Override
	public ILongBinaryOperator getOperator() {
		return operator;
	}

	@Override
	public String toString() {
		return "SingleColumnAggregationLogic [measureIndex=" + measureIndex + ", operator=" + operator + "]";
	}

	@Override
	public long aggregateToLong(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice) {
		long neutral = operator.neutralAsLong();

		// The following is discarded due to early optimization
		// return alternativeAggregateTo(roaringCube, rowsIterator, neutral);

		PrimitiveIterator.OfLong aggregateIt = measuresTable.readLong(rowsIterator, measureIndex);

		// Initialize ourself with the aggregation neutral element
		long aggregate = neutral;

		while (aggregateIt.hasNext()) {
			long next = aggregateIt.nextLong();

			// Aggregate next value
			aggregate = operator.applyAsLong(next, aggregate);
		}

		return aggregate;
	}

	// This would enable easily concurrent aggregation
	protected double alternativeAggregateTo(IHolyMeasuresTable measuresTable,
			PrimitiveIterator.OfLong rowsIterator,
			long neutral) {
		AtomicLong ref = new AtomicLong(neutral);

		measuresTable.acceptLongs(rowsIterator, measureIndex, next -> {
			boolean cas = false;

			do {
				long currentAggregate = ref.get();
				long newAggregate = operator.applyAsLong(currentAggregate, next);

				cas = ref.compareAndSet(currentAggregate, newAggregate);
			} while (!cas);
		});

		return ref.get();
	}

}
