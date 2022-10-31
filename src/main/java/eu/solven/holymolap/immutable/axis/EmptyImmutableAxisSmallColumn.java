package eu.solven.holymolap.immutable.axis;

import java.util.function.Consumer;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.mutable.dictionary.IAxisSmallDictionary;

public class EmptyImmutableAxisSmallColumn implements IScannableAxisSmallColumn {
	private static final Logger LOGGER = LoggerFactory.getLogger(EmptyImmutableAxisSmallColumn.class);

	@Override
	public void acceptCoordinates(Consumer<Object> coordinateConsumer) {
		LOGGER.trace("Empty");
	}

	@Override
	public long getRows() {
		return 0;
	}

	@Override
	public long getBrokenRows() {
		return 0;
	}

	@Override
	public int getCoordinateRef(long cellIndex) {
		return IAxisSmallDictionary.NO_COORDINATE_INDEX;
	}

	@Override
	public RoaringBitmap getCoordinateBitmap(long l) {
		return new RoaringBitmap();
	}
}
