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
import eu.solven.holymolap.query.ICountMeasuresConstants;
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
		// countStar is processed separately as it may be added implicitly on emptyCubes, while missing on not-empty
		// cubes. Start by checking it is present in all cubes
		boolean allHasCountStar = partitions().stream()
				.allMatch(c -> c.getMeasuresTable()
						.getMeasuresDefinition()
						.findMeasureIndex(ICountMeasuresConstants.COUNT_MEASURE) >= 0);

		List<IMeasuredAxis> measures = partitions().stream()
				.flatMap(hc -> hc.getMeasuresTable().getMeasuresDefinition().measures().stream())
				.map(IHolyMeasureColumnMeta::asMeasuredAxis)
				// We exclude countStar if it not present in all cubes
				.filter(m -> allHasCountStar ? true : !m.equals(ICountMeasuresConstants.COUNT_MEASURE))
				.distinct()
				.collect(Collectors.toList());

		return new HolyMeasuresTableDefinition(measures);
	}

}
