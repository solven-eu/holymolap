package eu.solven.holymolap.cube.mutable;

public interface IHolyMutable {
	/**
	 * Once locked, given structure should not change anymore. Any read-operation (e.g. to fill an external structure)
	 * may/should lock.
	 * 
	 * @return
	 */
	boolean isLocked();
}
