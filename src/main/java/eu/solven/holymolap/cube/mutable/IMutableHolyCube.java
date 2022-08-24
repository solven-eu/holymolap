package eu.solven.holymolap.cube.mutable;

import java.util.Map;
import java.util.stream.Stream;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;

/**
 * A mutable {@link IHolyCube}, accepting new rows, contributing into new or existing cells.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableHolyCube {

	/**
	 * 
	 * @return a Live view of the building axes.
	 */
	// IHasAxesWithCoordinates getAxes();

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

	void acceptRowToCell(Stream<? extends IHolyCubeRecord> toAdd);

	/**
	 * Will flush for pending operations, and return an immutable {@link IHolyCube}
	 * 
	 * @return {@link IHolyCube} holding the data of current {@link IMutableHolyCube}
	 */
	IHolyCube closeToHolyCube();
}
