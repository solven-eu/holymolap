package eu.solven.holymolap.compression.doubles;

import java.util.Arrays;
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
public class DictionaryDoubleList extends AbstractDoubleList implements IHasMemoryFootprint {
	final double[] indexToDouble;
	final int[] rowToIndex;

	public DictionaryDoubleList(double[] array) {
		// TODO We should ensure most frequent doubles are first
		// It will enable better compression of the rowToIndex structure
		indexToDouble = DoubleStream.of(array).distinct().toArray();
		rowToIndex = DoubleStream.of(array).mapToInt(d -> Arrays.binarySearch(indexToDouble, d)).toArray();
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
