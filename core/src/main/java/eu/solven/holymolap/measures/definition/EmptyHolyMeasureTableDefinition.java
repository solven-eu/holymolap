package eu.solven.holymolap.measures.definition;

import java.util.Collections;
import java.util.List;

import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * An empty {@link IHolyMeasuresDefinition}
 * 
 * @author Benoit Lacelle
 *
 */
public class EmptyHolyMeasureTableDefinition implements IHolyMeasuresDefinition {

	@Override
	public List<IHolyMeasureColumnMeta> measures() {
		return Collections.emptyList();
	}

	@Override
	public int findMeasureIndex(IMeasuredAxis aggregatesAxis) {
		return -1;
	}

}
