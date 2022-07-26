package eu.solven.holymolap.cube.mutable;

import java.util.function.Consumer;

public interface IScannableAxisColumn {

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
}
