package eu.solven.holymolap.mutable.column;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;
import eu.solven.holymolap.immutable.column.IScannableDoubleMeasureColumn;

/**
 * Enable accumulating doubles
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableDoubleAggregatesColumn extends IMutableAggregatesColumn {
	@Override
	@Deprecated
	default void aggregateObject(int cellIndex, Object object) {
		double asDouble;
		if (object instanceof Number) {
			asDouble = ((Number) object).doubleValue();
		} else if (object instanceof CharSequence) {
			try {
				asDouble = FastDoubleParser.parseDouble((CharSequence) object);
			} catch (RuntimeException e) {
				// e.printStackTrace();
				// TODO Record how many times we get there as this is some sort of Assertion failed
				asDouble = Double.NaN;
			}
		} else {
			// TODO Record how many times we get there as this is some sort of Assertion failed
			asDouble = Double.NaN;
		}
		aggregateDouble(cellIndex, asDouble);
	}

	void aggregateDouble(int rowIndex, double contribution);

	@Override
	IScannableDoubleMeasureColumn flush();

}
