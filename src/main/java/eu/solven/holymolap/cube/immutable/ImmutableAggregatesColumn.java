package eu.solven.holymolap.cube.immutable;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import eu.solven.holymolap.cube.mutable.IMutableAxisColumn;
import eu.solven.holymolap.cube.mutable.IScannableAxisColumn;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import me.lemire.integercompression.IntCompressor;

/**
 * A column of coordinates. Each row is typically associated to a cell (i.e. a {@link Set} of coordinates).
 * 
 * @author Benoit Lacelle
 *
 */
// https://lemire.me/blog/2017/11/10/how-should-you-build-a-high-performance-column-store-for-the-2020s/
public class ImmutableAggregatesColumn implements IScannableAxisColumn {
	final List<Object> indexToCoordinate;
	final int[] compressedRowToCoordinate;

	final AtomicLong brokenRows = new AtomicLong();

	// Move into ThreadLocal?
	final IntCompressor iic = new IntCompressor();

	protected ImmutableAggregatesColumn(IMutableAxisColumn input) {
		Object2IntMap<Object> coordinateToIndex = input.getCoordinateToIndex().asObject2Int();
		Object[] rawIndexToCoordinate = new Object[coordinateToIndex.size()];
		coordinateToIndex.object2IntEntrySet().forEach(e -> rawIndexToCoordinate[e.getIntValue()] = e.getIntValue());
		this.indexToCoordinate = Arrays.asList(rawIndexToCoordinate);

		this.compressedRowToCoordinate = iic.compress(input.getRowToIndex());

		this.brokenRows.set(input.getBrokenRows());
	}

	@Override
	public long getRows() {
		return compressedRowToCoordinate.length;
	}

	@Override
	public long getBrokenRows() {
		return brokenRows.get();
	}

	@Override
	public void acceptCoordinates(Consumer<Object> coordinateConsumer) {
		int[] rowToCoordinates;
		synchronized (this) {
			rowToCoordinates = iic.uncompress(compressedRowToCoordinate);
		}

		IntStream.of(rowToCoordinates).forEach(coordinate -> {
			coordinateConsumer.accept(indexToCoordinate.get(coordinate));
		});
	}

}
