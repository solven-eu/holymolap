package eu.solven.holymolap.stable.v1;

/**
 * This interface enables simplest queries. It is synchronous, hence should be reserved for simple queries.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IOneShotAggregator {
	IAggregationResult aggregate(IAggregationQuery query);
}
