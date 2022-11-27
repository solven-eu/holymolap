package eu.solven.holymolap.exception;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * Helps working with edge-cases, returning empty data-structures.
 * 
 * This may cover legit cases (e.g. an empty {@link IHolyCube} would not refer any axis), or seemingly error cases (e.g.
 * referring to a not-existing axis due to a typo).
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyExceptionManagement {
	protected HolyExceptionManagement() {
		// hidden
	}

	// TODO Return an Immutable thing!
	public static RoaringBitmap immutableEmptyBitmap() {
		return new RoaringBitmap();
	}
}
