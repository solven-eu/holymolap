package eu.solven.holymolap.tools;

import org.springframework.jmx.export.annotation.ManagedAttribute;

/**
 * Enable monitoring the memory footprint of current {@link Object}
 * 
 * @author Benoit Lacelle
 *
 */
public interface IHasMemoryFootprint {

	/**
	 * 
	 * @return the size in bytes. Long.MAX_VALUE if not implemented
	 */
	@ManagedAttribute
	default long getSizeInBytes() {
		return Long.MAX_VALUE;
	}
}
