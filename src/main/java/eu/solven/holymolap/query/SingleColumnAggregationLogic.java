package eu.solven.holymolap.query;

import org.roaringbitmap.IntIterator;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.stable.v1.IDoubleBinaryOperator;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

public class SingleColumnAggregationLogic implements IAggregationLogic<Double> {
	protected final String aggregatedKey;
	protected final IDoubleBinaryOperator operator;

	public SingleColumnAggregationLogic(String aggregatedKey, IDoubleBinaryOperator operator) {
		this.aggregatedKey = aggregatedKey;
		this.operator = operator;
	}

	@Override
	public Double aggregateTo(IHolyCube roaringCube, IntIterator it, int[] valueIndexes) {
		// Initialize ourself with the aggregation neutral element
		double neutral = operator.neutral();

		DoubleIterator doubleIt = roaringCube.readDouble(it, roaringCube.getIndex().getKeyIndex(aggregatedKey), neutral);

		double aggregate = neutral;

		while (doubleIt.hasNext()) {
			double next = doubleIt.nextDouble();

			// Aggregate next value
			aggregate = operator.applyAsDouble(next, aggregate);
		}

		return aggregate;
	}

}
