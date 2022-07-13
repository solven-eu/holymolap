package eu.solven.holymolap.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A aggregation query.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IAggregateQuery {

	/**
	 * The filters of current query. A filter refers to the condition for the data to be included. An empty {@link List}
	 * means the whole data has to be included.
	 * 
	 * @return a list of filters.
	 */
	List<? extends Map<String, ?>> getFilters();

	/**
	 * The wildcards represent the column amongst which the reslut has to be ventilated.
	 * 
	 * @return a Set of columns
	 */
	Set<String> getWildcards();

}
