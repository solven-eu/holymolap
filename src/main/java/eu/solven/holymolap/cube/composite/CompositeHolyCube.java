package eu.solven.holymolap.cube.composite;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasureColumnMeta;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.stable.v1.IMeasuredAxis;

/**
 * Build an {@link ICompositeHolyCube} given a {@link Map} or {@link List} of {@link IHolyCube}
 * 
 * @author Benoit Lacelle
 *
 */
public class CompositeHolyCube implements ICompositeHolyCube {
	final Map<String, IHolyCube> partitions;

	public CompositeHolyCube(Map<String, IHolyCube> partitions) {
		this.partitions = ImmutableMap.copyOf(partitions);
	}

	public CompositeHolyCube(IHolyCube... holyCubes) {
		this(IntStream.range(0, holyCubes.length)
				.mapToObj(i -> i)
				.collect(Collectors.toMap(i -> Integer.toString(i), i -> holyCubes[i])));
	}

	@Override
	public Collection<IHolyCube> partitions() {
		return partitions.values();
	}

	@Override
	public IHolyMeasuresDefinition getMeasuresDefinition() {
		List<IMeasuredAxis> measures = partitions().stream()
				.flatMap(hc -> hc.getMeasuresTable().getMeasuresDefinition().measures().stream())
				.map(IHolyMeasureColumnMeta::asMeasuredAxis)
				.distinct()
				.collect(Collectors.toList());

		return new HolyMeasuresTableDefinition(measures);
	}

}
