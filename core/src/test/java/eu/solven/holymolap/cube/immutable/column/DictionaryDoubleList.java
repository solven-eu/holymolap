package eu.solven.holymolap.cube.immutable.column;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} which is RAM-efficient if the distinct count of double is very low.
 * 
 * @author Benoit Lacelle
 *
 */
public class DictionaryDoubleList extends AbstractDoubleList {
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
}
