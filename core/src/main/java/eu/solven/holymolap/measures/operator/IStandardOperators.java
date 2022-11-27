package eu.solven.holymolap.measures.operator;

public interface IStandardOperators {

	String SUM = "SUM";
	/**
	 * Count the number of considered input records (similarly to SQL)
	 */
	String COUNT = "COUNT";
	/**
	 * Count the number of cells considered in the query. It helps understanding the granularity of the considered data,
	 * or the presence/lack of intermediate cubes.
	 */
	String CELLCOUNT = "CELLCOUNT";

	@Deprecated(since = "avg should be computed as the ratio of SUM / COUNT")
	String AVG = "AVG";

}
