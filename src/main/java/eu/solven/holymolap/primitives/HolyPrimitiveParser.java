package eu.solven.holymolap.primitives;

import ch.randelshofer.fastdoubleparser.FastDoubleParser;

/**
 * Helps parsing primitives.
 * 
 * @author Benoit Lacelle
 *
 */
public class HolyPrimitiveParser {
	protected HolyPrimitiveParser() {
		// hidden
	}

	public static double toDouble(Object object) {
		double asDouble;
		if (object instanceof Number) {
			asDouble = ((Number) object).doubleValue();
		} else if (object instanceof CharSequence) {
			try {
				asDouble = toDouble((CharSequence) object);
			} catch (RuntimeException e) {
				// e.printStackTrace();
				// TODO Record how many times we get there as this is some sort of Assertion failed
				asDouble = Double.NaN;
			}
		} else {
			// TODO Record how many times we get there as this is some sort of Assertion failed
			asDouble = Double.NaN;
		}
		return asDouble;
	}

	private static double toDouble(CharSequence charSequence) {
		return FastDoubleParser.parseDouble(charSequence);
	}
}
