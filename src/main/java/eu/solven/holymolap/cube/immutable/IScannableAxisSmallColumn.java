package eu.solven.holymolap.cube.immutable;

import java.util.function.Consumer;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.tools.IHasMemoryFootprint;

public interface IScannableAxisSmallColumn extends IHasMemoryFootprint {

	@Deprecated(since = "Is this useful?")
	void acceptCoordinates(Consumer<Object> coordinateConsumer);

	/**
	 * 
	 * @return The number of rows in given column.
	 */
	long getRows();

	/**
	 * An issue, may be one tried to append an incompatible type (a String into a Double column).
	 * 
	 * @return the number of rows having encountered an issue.
	 */
	long getBrokenRows();

	// Replace this usage by something needing to scan the column?
	int getCoordinateRef(long cellIndex);

	RoaringBitmap getCoordinateBitmap(long l);
}
