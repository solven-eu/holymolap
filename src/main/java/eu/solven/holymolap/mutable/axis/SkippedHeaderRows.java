package eu.solven.holymolap.mutable.axis;

import java.util.Arrays;

import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;

/**
 * Append nbRows as noCoordinateRef at the beginning of the column. It is useful when a new axes is discovered after
 * having inserted multiple rows.
 * 
 */
public class SkippedHeaderRows implements IMutableAxisSmallColumn {
	final int nbSkipped;
	final IMutableAxisSmallColumn decorated;

	public SkippedHeaderRows(int nbSkipped, IMutableAxisSmallColumn decorated) {
		this.nbSkipped = nbSkipped;
		this.decorated = decorated;
	}

	@Override
	public IMutableAxisSmallDictionarySink getCoordinateToRef() {
		return decorated.getCoordinateToRef();
	}

	@Override
	public void getRowToIndex(int from, int a[], int offset, int length) {
		if (offset != 0) {
			throw new UnsupportedOperationException("TODO");
		}

		int skippedLength;
		if (from < nbSkipped) {
			skippedLength = Math.min(length, nbSkipped - from);

			Arrays.fill(a, offset, offset + skippedLength, IAxisSmallDictionary.NO_COORDINATE_INDEX);
		} else {
			skippedLength = 0;
		}

		if (from + length > nbSkipped) {
			int fromDecorated = Math.max(0, from - nbSkipped);
			int lengthDecorated = from + length - nbSkipped;
			decorated.getRowToIndex(fromDecorated, a, offset + skippedLength, lengthDecorated);
		}
	}

	@Override
	public void appendCoordinate(Object coordinate) {
		decorated.appendCoordinate(coordinate);
	}

	@Override
	public void appendCoordinateRef(int coordinateRef) {
		decorated.appendCoordinateRef(coordinateRef);
	}

	@Override
	public long getRows() {
		return nbSkipped + decorated.getRows();
	}

	@Override
	public long getBrokenRows() {
		return decorated.getBrokenRows();
	}

}
