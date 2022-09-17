package eu.solven.holymolap.cube.measures;

import eu.solven.holymolap.query.IAggregationLogic;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * An {@link IHolyMeasureColumnMeta} expresses the {@link IAggregationLogic} to apply to given column. It is typically
 * stored into an {@link IHolyMeasuresTable}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHolyMeasureColumnMeta {

	default String getColumn() {
		return asMeasuredAxis().getAxis();
	}

	IAggregationLogic<?> getAggregationLogic();

	IMeasuredAxis asMeasuredAxis();
}
