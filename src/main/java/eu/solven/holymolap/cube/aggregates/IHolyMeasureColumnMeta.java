package eu.solven.holymolap.cube.aggregates;

import eu.solven.holymolap.query.IAggregationLogic;
import eu.solven.holymolap.stable.v1.IAggregatedAxis;

/**
 * An {@link IHolyMeasureColumnMeta} expresses the {@link IAggregationLogic} to apply to given column. It is
 * typically stored into an {@link IHolyMeasureTable}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasureColumnMeta {

	String getColumn();

	IAggregationLogic<?> getAggregationLogic();

	IAggregatedAxis asAggregatedAxis();
}
