package eu.solven.holymolap.sink.record;

import java.util.Collections;
import java.util.List;

public class EmptyHolyRecord implements IHolyRecord {

	public static EmptyHolyRecord INSTANCE = new EmptyHolyRecord();

	@Override
	public List<String> getAxes() {
		return Collections.emptyList();
	}

	@Override
	public void accept(IHolyRecordVisitor visitor) {
		// nothing to visit as empty
	}

}
