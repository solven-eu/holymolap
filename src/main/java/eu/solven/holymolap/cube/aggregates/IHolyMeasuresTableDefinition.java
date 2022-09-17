package eu.solven.holymolap.cube.aggregates;

import java.util.List;

import eu.solven.holymolap.stable.v1.IAggregatedAxis;

/**
 * Describes a set of measures
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasuresTableDefinition {
	/**
	 * 
	 * @return the (distinct) ordered measures
	 */
	List<IHolyMeasureColumnMeta> measures();

	int getMeasureIndex(IHolyMeasureColumnMeta measure);

	int findMeasureIndex(IAggregatedAxis aggregatesAxis);
}
