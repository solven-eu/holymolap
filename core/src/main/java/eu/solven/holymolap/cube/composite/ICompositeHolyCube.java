package eu.solven.holymolap.cube.composite;

import java.util.Collection;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHasMeasuresDefinition;
import eu.solven.holymolap.tools.IHasMemoryFootprint;

/**
 * An {@link ICompositeHolyCube} is {@link Collection} of {@link IHolyCube}. We do try merging them into a single big
 * {@link IHolyCube} it would mean consuming more memory and less concurrent design, and deep complexities (like what's
 * a bitmap of cells over a composite cellSet?).
 * 
 * @author Benoit Lacelle
 *
 */
public interface ICompositeHolyCube extends IHasMemoryFootprint, IHasMeasuresDefinition {

	Collection<IHolyCube> partitions();

	default long getNbRows() {
		return partitions().stream().mapToLong(IHolyCube::getNbRows).sum();
	}

	@Override
	default public long getSizeInBytes() {
		return partitions().stream().mapToLong(IHolyCube::getSizeInBytes).sum();
	}

}
