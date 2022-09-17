package eu.solven.holymolap.cube.immutable;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.roaringbitmap.RoaringBitmap;

import eu.solven.holymolap.cube.mutable.IAxisSmallDictionary;
import eu.solven.holymolap.cube.mutable.IMutableAxisSmallColumn;
import me.lemire.integercompression.IntCompressor;

/**
 * A column of coordinates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableAxisColumn implements IScannableAxisSmallColumn {
	final int nbRows;

	final IAxisCoordinatesDictionary axisCoordinatesDictionary;
	final int[] compressedRowToCoordinate;

	final AtomicLong brokenRows = new AtomicLong();

	// Move into ThreadLocal?
	final IntCompressor iic = new IntCompressor();

	public ImmutableAxisColumn(IAxisCoordinatesDictionary axisCoordinatesDictionary, IMutableAxisSmallColumn input) {
		this.axisCoordinatesDictionary = axisCoordinatesDictionary;

		int[] rowToIndex = input.getRowToIndex();

		nbRows = rowToIndex.length;

		this.compressedRowToCoordinate = iic.compress(rowToIndex);

		this.brokenRows.set(input.getBrokenRows());
	}

	@Override
	public long getBrokenRows() {
		return brokenRows.get();
	}

	private int[] uncompressRowToCoordinate() {
		int[] rowToCoordinates;
		synchronized (this) {
			rowToCoordinates = iic.uncompress(compressedRowToCoordinate);
		}
		return rowToCoordinates;
	}

	@Override
	public long getRows() {
		return nbRows;
	}

	@Override
	public void acceptCoordinates(Consumer<Object> coordinateConsumer) {
		int[] rowToCoordinates = uncompressRowToCoordinate();

		IntStream.of(rowToCoordinates).forEach(coordinateRef -> {
			Object coordinate = axisCoordinatesDictionary.getCoordinate(coordinateRef);
			coordinateConsumer.accept(coordinate);
		});
	}

	@Override
	public long getSizeInBytes() {
		return compressedRowToCoordinate.length * 4;
	}

	@Override
	public int getCoordinateRef(long cellIndex) {
		if (cellIndex > Integer.MAX_VALUE) {
			// No need to throw, we can simply say there is no coordinate
			return IAxisSmallDictionary.NO_COORDINATE_INDEX;
		}

		int[] rowToCoordinates = uncompressRowToCoordinate();

		return rowToCoordinates[(int) cellIndex];
	}

	@Override
	public RoaringBitmap getCoordinateBitmap(long coordinateRef) {
		int[] rowToCoordinates = uncompressRowToCoordinate();

		// TODO If the bitmap is lighter, save it in-memory as replacement for compressedRowToCoordinate
		RoaringBitmap bitmap = new RoaringBitmap();

		for (int i = 0; i < nbRows; i++) {
			if (rowToCoordinates[i] == coordinateRef) {
				bitmap.add(i);
			}
		}

		return bitmap;
	}

}
