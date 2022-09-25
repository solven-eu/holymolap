package eu.solven.holymolap.cube.measures;

import java.util.List;

import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * Describes a {@link List} of measures
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasuresDefinition {
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
