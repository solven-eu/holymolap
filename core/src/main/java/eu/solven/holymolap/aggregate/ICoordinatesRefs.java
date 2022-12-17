package eu.solven.holymolap.aggregate;

import eu.solven.holymolap.immutable.axes.IHasAxesWithCoordinates;

/**
 * This describe a slice of a Cube. It may be a cell, the whole cube, or any intermediate simple slice. By simple we
 * mean a slice with a single coordinate bet expressed axis.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ICoordinatesRefs {

	IHasAxesWithCoordinates getAxesWithCoordinates();

	int[] getAxesIndexes();

	long[] getCoordinatesRef();

}
