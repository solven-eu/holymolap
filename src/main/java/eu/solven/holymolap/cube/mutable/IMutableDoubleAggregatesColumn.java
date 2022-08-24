package eu.solven.holymolap.cube.mutable;

import eu.solven.holymolap.cube.immutable.IScannableDoubleAggregatesColumn;

/**
 * Enable accumulating doubles
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableDoubleAggregatesColumn {

	void aggregateRow(int rowIndex, double contribution);

	IScannableDoubleAggregatesColumn flush();

}
