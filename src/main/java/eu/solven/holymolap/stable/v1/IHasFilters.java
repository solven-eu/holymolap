package eu.solven.holymolap.stable.v1;

import java.util.List;
import java.util.Map;

/**
 * A {@link List} of filters. Typically used by {@link IAggregationQuery}, or {@link IExclusionFilter}.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasFilters {

	/**
	 * An empty {@link Map} would match any rows.
	 * 
	 * @return the {@link List} of filters. To be interpreted as an OR over AND conditions.
	 */
	IAxesFilter getFilters();
}
