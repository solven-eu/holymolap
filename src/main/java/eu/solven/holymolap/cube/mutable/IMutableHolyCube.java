package eu.solven.holymolap.cube.mutable;

import java.util.Map;

import eu.solven.holymolap.cube.IHasAxesWithCoordinates;
import eu.solven.holymolap.cube.IHolyCube;

/**
 * A mutable {@link IHolyCube}, accepting new rows, contributing into new or existing cells.
 * 
 * @author Benoit Lacelle
 *
 */
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

	/**
	 * 
	 * @return a Live view of the building axes.
	 */
	IHasAxesWithCoordinates getAxes();
}
