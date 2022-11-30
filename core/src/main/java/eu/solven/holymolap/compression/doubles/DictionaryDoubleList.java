package eu.solven.holymolap.compression.doubles;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.DoubleStream;

import eu.solven.holymolap.tools.IHasMemoryFootprint;
import eu.solven.pepper.memory.IPepperMemoryConstants;
import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which is RAM-efficient if the distinct count of double is very low.
 * 
 * @author Benoit Lacelle
 *
 */
// To be memory efficient, we need indexToDouble to be at most 4 times smaller than the input array
public class DictionaryDoubleList extends AbstractDoubleList implements IHasMemoryFootprint {
	final double[] indexToDouble;
	final int[] rowToIndex;

	public DictionaryDoubleList(double[] array) {
		// TODO We should ensure most frequent doubles are first
		// It will enable better compression of the rowToIndex structure
		indexToDouble = DoubleStream.of(array).distinct().toArray();
		rowToIndex = DoubleStream.of(array).mapToInt(d -> Arrays.binarySearch(indexToDouble, d)).toArray();
	}

	public static Optional<DictionaryDoubleList> tryMake(double[] array) {
		int safeLimit = 16 * 1024;
		if (array.length < safeLimit) {
			// It is small enough: we can safely return an instance
			return Optional.of(new DictionaryDoubleList(array));
		}

		// The input array is getting big: just materializing the distinct set can be expensive
		long disctintCountWithin1k = DoubleStream.of(array).limit(safeLimit).distinct().count();
		if (disctintCountWithin1k >= disctintCountWithin1k * 0.01) {
			// There is more than 1% of variance: we prefer not trying materializing the DictionaryDoubleList
			return Optional.empty();
		}

		return Optional.of(new DictionaryDoubleList(array));
	}

	@Override
	public double getDouble(int index) {
		return indexToDouble[index];
	}

	@Override
	public int size() {
		return rowToIndex.length;
	}

	@Override
	public long getSizeInBytes() {
		long size = 0L;

		size += IPepperMemoryConstants.INT * rowToIndex.length;
		size += IPepperMemoryConstants.DOUBLE * indexToDouble.length;

		return size;
	}
}
