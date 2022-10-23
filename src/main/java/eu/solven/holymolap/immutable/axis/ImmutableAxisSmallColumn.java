package eu.solven.holymolap.immutable.axis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.roaringbitmap.RoaringBitmap;

import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;

import eu.solven.holymolap.cache.CompressedIntArray;
import eu.solven.holymolap.immutable.dictionary.IAxisCoordinatesDictionary;
import eu.solven.holymolap.mutable.axis.IMutableAxisSmallColumn;
import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;

/**
 * A column of coordinates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableAxisSmallColumn implements IScannableAxisSmallColumn {
	final int nbRows;

	final IAxisCoordinatesDictionary axisCoordinatesDictionary;
	final CompressedIntArray compressedRowToCoordinate;

	// https://stackoverflow.com/questions/264582/is-there-a-softhashmap-in-java
	// http://jeremymanson.blogspot.com/2009/07/how-hotspot-decides-to-clear_07.html
	// final ConcurrentReferenceHashMap<Long, RoaringBitmap>
	final LoadingCache<Long, RoaringBitmap> coordinateRefToBitmap = CacheBuilder.newBuilder()
			.concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
			.softValues()
			.build(CacheLoader.from(coordinateRef -> {
				return getCoordinateBitmapNoCache(coordinateRef);
			}));

	final AtomicLong brokenRows = new AtomicLong();

	public ImmutableAxisSmallColumn(IAxisCoordinatesDictionary axisCoordinatesDictionary,
			IMutableAxisSmallColumn input) {
		this.axisCoordinatesDictionary = axisCoordinatesDictionary;

		nbRows = Ints.checkedCast(input.getRows());
		int[] rowToIndex = new int[nbRows];
		input.getRowToIndex(0, rowToIndex, 0, nbRows);

		this.compressedRowToCoordinate = new CompressedIntArray(rowToIndex);

		this.brokenRows.set(input.getBrokenRows());
	}

	@Override
	public long getBrokenRows() {
		return brokenRows.get();
	}

	@Override
	public long getRows() {
		return nbRows;
	}

	@Override
	public void acceptCoordinates(Consumer<Object> coordinateConsumer) {
		int[] rowToCoordinates = compressedRowToCoordinate.getIntArray();

		IntStream.of(rowToCoordinates).forEach(coordinateRef -> {
			Object coordinate = axisCoordinatesDictionary.getCoordinate(coordinateRef);
			coordinateConsumer.accept(coordinate);
		});
	}

	@Override
	public long getSizeInBytes() {
		return compressedRowToCoordinate.getSizeInBytes();
	}

	@Override
	public int getCoordinateRef(long cellIndex) {
		if (cellIndex > Integer.MAX_VALUE) {
			// No need to throw, we can simply say there is no coordinate
			return IAxisSmallDictionary.NO_COORDINATE_INDEX;
		}

		int[] rowToCoordinates = compressedRowToCoordinate.getIntArray();

		return rowToCoordinates[Ints.checkedCast(cellIndex)];
	}

	@Override
	public RoaringBitmap getCoordinateBitmap(long coordinateRef) {
		return coordinateRefToBitmap.getUnchecked(coordinateRef);
	}

	protected RoaringBitmap getCoordinateBitmapNoCache(long coordinateRef) {
		int[] rowToCoordinates = compressedRowToCoordinate.getIntArray();

		// TODO If the bitmap is lighter, save it in-memory as replacement for compressedRowToCoordinate
		RoaringBitmap bitmap = new RoaringBitmap();

		for (int i = 0; i < nbRows; i++) {
			if (rowToCoordinates[i] == coordinateRef) {
				bitmap.add(i);
			}
		}

		return bitmap;
	}

	@Override
	public String toString() {
		List<Object> firstCoordinates = new ArrayList<>(100);
		acceptCoordinates(coordinate -> {
			if (firstCoordinates.size() < 100) {
				firstCoordinates.add(coordinate);
			}
		});

		return MoreObjects.toStringHelper(this)
				.add("nbRows", getRows())
				.add("firstCoordinates", firstCoordinates)
				.toString();
	}
}
