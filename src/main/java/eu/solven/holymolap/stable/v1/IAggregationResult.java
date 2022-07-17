package eu.solven.holymolap.stable.v1;

import java.util.NavigableMap;

import eu.solven.holymolap.query.operator.IDoubleAggregate;

/**
 * The result of an {@link IAggregationQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregationResult {
	long nbPoints();

	NavigableMap<? extends Comparable<?>, ? extends Comparable<?>> getPointNiceCoordinate(int pointIndex);

	IDoubleAggregate getPointNiceCoordinate(int pointIndex, Comparable<?> key, IDoubleBinaryOperator operator);
}
