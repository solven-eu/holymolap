package eu.solven.holymolap.sink.record;

import java.util.List;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

public interface IHolyRecordsTableVisitor {
	/**
	 * 
	 * @param axisIndex
	 * @param listObjects
	 *            may be larger than the {@link IHolyRecordsTable} size. Do not consider these additional entries.
	 */
	void onObject(int axisIndex, List<?> listObjects);

	/**
	 * 
	 * @param axisIndex
	 * @param l
	 *            may be larger than the {@link IHolyRecordsTable} size. Do not consider these additional entries.
	 */
	default void onInt(int axisIndex, int[] i) {
		onObject(axisIndex, IntArrayList.wrap(i));
	};

	/**
	 * 
	 * @param axisIndex
	 * @param l
	 *            may be larger than the {@link IHolyRecordsTable} size. Do not consider these additional entries.
	 */
	default void onLong(int axisIndex, long[] l) {
		onObject(axisIndex, LongArrayList.wrap(l));
	};

	/**
	 * 
	 * @param axisIndex
	 * @param d
	 *            may be larger than the {@link IHolyRecordsTable} size. Do not consider these additional entries.
	 */
	default void onDouble(int axisIndex, double[] d) {
		onObject(axisIndex, DoubleArrayList.wrap(d));
	};
}
