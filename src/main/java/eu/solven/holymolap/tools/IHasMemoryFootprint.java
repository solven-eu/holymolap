package eu.solven.holymolap.tools;

/**
 * Enable monitoring the memory footprint of current {@link Object}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasMemoryFootprint {

	long getSizeInBytes();
}
