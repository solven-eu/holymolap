package eu.solven.holymolap.result;
//package solven.roaringcube.result;
//
//import java.util.NavigableMap;
//
//import solven.roaringcube.IRoaringCube;
//import solven.roaringcube.query.operator.IDoubleAggregate;
//import solven.roaringcube.query.operator.IDoubleBinaryOperator;
//
//public class AggregationResult implements IAggregationResult {
//
//	protected final int nbPoints;
//	protected final IRoaringCube roaringCube;
//
//	public AggregationResult(int nbPoints, IRoaringCube roaringCube) {
//		this.nbPoints = nbPoints;
//	}
//
//	@Override
//	public int nbPoints() {
//		return nbPoints;
//	}
//
//	@Override
//	public NavigableMap<? extends Comparable<?>, ? extends Comparable<?>> getPointNiceCoordinate(int pointIndex) {
//		return roaringCube.convertToCoordinates(row, keys);
//	}
//
//	@Override
//	public IDoubleAggregate getPointNiceCoordinate(int pointIndex, Comparable<?> key, IDoubleBinaryOperator operator) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//}
