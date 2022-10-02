package eu.solven.holymolap.sink.record;

import java.util.Collections;
import java.util.List;

public class EmptyHolyRecord implements IHolyRecord {

	public static EmptyHolyRecord INSTANCE = new EmptyHolyRecord();

	// @Override
	// public int[] objectIndexes() {
	// return new int[0];
	// }
	//
	// @Override
	// public Object getObject(int axisIndex) {
	// throw new UnsupportedOperationException("Empty");
	// }
	//
	// @Override
	// public int[] intIndexes() {
	// return new int[0];
	// }
	//
	// @Override
	// public int getInt(int axisIndex) {
	// throw new UnsupportedOperationException("Empty");
	// }
	//
	// @Override
	// public int[] doubleIndexes() {
	// return new int[0];
	// }
	//
	// @Override
	// public double getDouble(int axisIndex) {
	// throw new UnsupportedOperationException("Empty");
	// }

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		// nothing to visit as empty
	}

}
