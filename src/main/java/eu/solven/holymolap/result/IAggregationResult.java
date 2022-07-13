package eu.solven.holymolap.result;

import java.util.NavigableMap;

import eu.solven.holymolap.query.IAggregateQuery;
import eu.solven.holymolap.query.operator.IDoubleAggregate;
import eu.solven.holymolap.query.operator.IDoubleBinaryOperator;

/**
 * The result of an {@link IAggregateQuery}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregationResult {
	long nbPoints();

	NavigableMap<? extends Comparable<?>, ? extends Comparable<?>> getPointNiceCoordinate(int pointIndex);

	IDoubleAggregate getPointNiceCoordinate(int pointIndex, Comparable<?> key, IDoubleBinaryOperator operator);
}
