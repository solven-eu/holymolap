package eu.solven.holymolap.sink;

import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.aggregates.IHolyAggregateTableDefinition;
import eu.solven.holymolap.cube.mutable.IMutableHolyCube;
import eu.solven.holymolap.cube.mutable.MutableHolyCube;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;

public class HolyCubeSink implements IHolyCubeSink {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCubeSink.class);

	final IHolyAggregateTableDefinition aggregations;

	public HolyCubeSink(IHolyAggregateTableDefinition aggregations) {
		this.aggregations = aggregations;
	}

	@Override
	public IHolyCube sink(ISinkContext context, Stream<? extends IHolyCubeRecord> toAdd) {
		IMutableHolyCube mutableHolyCube = new MutableHolyCube(aggregations);

		mutableHolyCube.acceptRowToCell(toAdd);

		return mutableHolyCube.closeToHolyCube();
	}
}
