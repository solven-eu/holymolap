package eu.solven.holymolap.tools;

import org.springframework.jmx.export.annotation.ManagedAttribute;

/**
 * Enable monitoring the memory footprint of current {@link Object}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasMemoryFootprint {

	@ManagedAttribute
	long getSizeInBytes();
}
