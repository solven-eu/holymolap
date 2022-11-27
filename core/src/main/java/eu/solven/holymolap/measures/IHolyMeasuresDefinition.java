package eu.solven.holymolap.measures;

import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * Describes a {@link List} of measures
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasuresDefinition {
	// WRONG: COUNT measures are implicit: computed given the cellSet, they are not materialized in the measuresTable
	// CELLCOUNT measures are implicit: computed given the cellSet, they are not materialized in the measuresTable
	int CELLCOUNT_MEASURE_INDEX = -2;

	/**
	 * 
	 * @return the (distinct) ordered measures
	 */
	List<IHolyMeasureColumnMeta> measures();

	int findMeasureIndex(IMeasuredAxis measuredAxis);

	default int getMeasureIndex(IHolyMeasureColumnMeta measure) {
		return findMeasureIndex(measure.asMeasuredAxis());
	}
}
