package eu.solven.holymolap.sink;

import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.cube.mutable.IMutableHolyCube;
import eu.solven.holymolap.cube.mutable.MutableHolyCube;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;

public class HolyCubeSink implements IHolyCubeSink {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCubeSink.class);

	final IHolyMeasuresDefinition measures;

	public HolyCubeSink(IHolyMeasuresDefinition measures) {
		this.measures = measures;
	}

	@Override
	public IHolyMeasuresDefinition getMeasures() {
		return measures;
	}

	@Override
	public IHolyCube sink(Stream<? extends IHolyCubeRecord> toAdd) {
		IMutableHolyCube mutableHolyCube = new MutableHolyCube(measures);

		mutableHolyCube.acceptRowToCell(toAdd);

		return mutableHolyCube.closeToHolyCube();
	}
}
