package eu.solven.holymolap.stable.v1;

import java.util.List;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * A {@link List} of columns. Typically used by {@link IAggregationQuery}, or {@link IHolyCube}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasColumns {

	/**
	 * @return a {@link List} of columns.
	 */
	List<String> getColumns();
}
