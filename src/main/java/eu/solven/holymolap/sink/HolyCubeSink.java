package eu.solven.holymolap.sink;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.mutable.cube.IMutableHolyCube;
import eu.solven.holymolap.mutable.cube.MutableHolyCube;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;

public class HolyCubeSink implements IHolyCubeSink {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HolyCubeSink.class);

	final IHolyMeasuresDefinition measures;

	final AtomicReference<IMutableHolyCube> pending = new AtomicReference<>();

	public HolyCubeSink(IHolyMeasuresDefinition measures) {
		this.measures = measures;
	}

	@Override
	public IHolyMeasuresDefinition getMeasures() {
		return measures;
	}

	@Override
	public IHolyCubeSink sink(Stream<? extends IHolyCubeRecord> toAdd) {
		while (pending.get() == null) {
			pending.compareAndSet(null, new MutableHolyCube(measures));
		}

		IMutableHolyCube mutableHolyCube = pending.get();

		mutableHolyCube.acceptRowToCell(toAdd);

		return this;
	}

	@Override
	public IHolyCube closeToHolyCube() {
		IMutableHolyCube mutableHolyCube = pending.get();

		if (mutableHolyCube == null) {
			// Not a single row has been sinked
			return new HolyCube();
		} else {
			return mutableHolyCube.closeToHolyCube();
		}

	}
}
