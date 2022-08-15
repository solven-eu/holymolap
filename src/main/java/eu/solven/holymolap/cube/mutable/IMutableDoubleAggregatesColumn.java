package eu.solven.holymolap.cube.mutable;

/**
 * Enable accumulating doubles
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableDoubleAggregatesColumn {

	void aggregateRow(int rowIndex, double contribution);

}
