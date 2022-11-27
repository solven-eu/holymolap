package eu.solven.holymolap.measures.aggregation;

import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.holymolap.aggregate.CoordinatesRefs;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.IHolyMeasuresTable;
import eu.solven.holymolap.stable.v1.IBinaryOperator;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;
import it.unimi.dsi.fastutil.longs.LongIterator;

public class ObjectAggregationLogic implements IAggregationLogic<Object> {
	protected final int measureIndex;
	protected final IBinaryOperator operator;

	public static IAggregationLogic<?> search(IHolyMeasuresDefinition definition, IMeasuredAxis measuredAxis) {
		int index = definition.findMeasureIndex(measuredAxis);

		if (index < 0) {
			throw new IllegalArgumentException("Can not find " + measuredAxis + " in " + definition);
		}

		return definition.measures().get(index).getAggregationLogic();
	}

	public ObjectAggregationLogic(int measureIndex, IBinaryOperator operator) {
		this.measureIndex = measureIndex;
		this.operator = operator;
	}

	@Override
	public IBinaryOperator getOperator() {
		return operator;
	}

	@Override
	public String toString() {
		return "SingleColumnAggregationLogic [measureIndex=" + measureIndex + ", operator=" + operator + "]";
	}

	@Override
	public Object aggregateTo(IHolyMeasuresTable measuresTable, LongIterator rowsIterator, CoordinatesRefs slice) {
		Object neutral = operator.neutral();

		// The following is discarded due to early optimization
		// return alternativeAggregateTo(roaringCube, rowsIterator, neutral);

		PrimitiveIterator.OfDouble doubleIt = measuresTable.readDouble(rowsIterator, measureIndex);

		// Initialize ourself with the aggregation neutral element
		Object aggregate = neutral;

		while (doubleIt.hasNext()) {
			double next = doubleIt.nextDouble();

			// Aggregate next value
			aggregate = operator.apply(next, aggregate);
		}

		return aggregate;
	}

	// This would enable easily concurrent aggregation
	protected Object alternativeAggregateTo(IHolyMeasuresTable measuresTable,
			LongIterator rowsIterator,
			double neutral) {
		AtomicReference<Object> doubleRef = new AtomicReference<>(neutral);

		measuresTable.acceptDoubles(rowsIterator, measureIndex, next -> {
			boolean cas = false;

			do {
				Object currentAggregate = doubleRef.get();
				Object newAggregate = operator.apply(currentAggregate, next);

				cas = doubleRef.compareAndSet(currentAggregate, newAggregate);
			} while (!cas);
		});

		return doubleRef.get();
	}

}
