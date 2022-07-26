package eu.solven.holymolap.cube.mutable;

import java.util.Map;

public interface IMutableHolyCube {

	/**
	 * 
	 * @param row
	 *            a row, to be contributed to either a new, or an existing cell.
	 */
	void acceptRowToCell(Map<String, ?> row);

	/**
	 * 
	 * @param aggregated
	 *            axis to aggregates
	 * @param groupBy
	 *            axis to coordinates
	 */
	void acceptRowToCell(Map<String, ?> aggregated, Map<String, ?> groupBy);

}
