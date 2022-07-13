package eu.solven.holymolap.comparable;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ordering;

/**
 * A {@link Comparator} which compare first the keyset, then the values
 * 
 * @author Benoit Lacelle
 * 
 */
public class NavigableMapComparator implements Comparator<NavigableMap<?, ?>>, Serializable {
	// http://findbugs.sourceforge.net/bugDescriptions.html#SE_COMPARATOR_SHOULD_BE_SERIALIZABLE
	private static final long serialVersionUID = 7928339315645573854L;

	protected static final Logger LOGGER = LoggerFactory.getLogger(NavigableMapComparator.class);

	public static final NavigableMapComparator INSTANCE = new NavigableMapComparator();

	@Override
	public int compare(NavigableMap<?, ?> o1, NavigableMap<?, ?> o2) {
		Iterator<? extends Entry<?, ?>> itKey1 = o1.entrySet().iterator();
		Iterator<? extends Entry<?, ?>> itKey2 = o2.entrySet().iterator();

		// First we compare keySet
		while (itKey1.hasNext() && itKey2.hasNext()) {
			Entry<?, ?> next1 = itKey1.next();
			Entry<?, ?> next2 = itKey2.next();

			int keyCompare = compareTo(next1.getKey(), next2.getKey());

			if (keyCompare != 0) {
				// key1 is before key2: then map1 is before map2
				return keyCompare;
			}
		}

		if (itKey1.hasNext()) {
			// then it2 does not have next: it2 is bigger: it1 is smaller
			return -1;
		} else if (itKey2.hasNext()) {
			// then it1 does not have next: it1 is bigger: it2 is smaller
			return 1;
		} else {
			// both it1 and it2 does not have next: then, their keys are equal:
			// do the same for values

			Iterator<? extends Entry<?, ?>> itValue1 = o1.entrySet().iterator();
			Iterator<? extends Entry<?, ?>> itValue2 = o2.entrySet().iterator();

			while (itValue1.hasNext() && itValue2.hasNext()) {
				Entry<?, ?> next1 = itValue1.next();
				Entry<?, ?> next2 = itValue2.next();

				int valueCompare = compareTo(next1.getValue(), next2.getValue());

				if (valueCompare != 0) {
					return valueCompare;
				}
			}

			// All keys equals and all values equals: Map are equals
			assert o1.equals(o2);

			return 0;
		}

	}

	@SuppressWarnings("rawtypes")
	protected int compareTo(Object left, Object right) {
		return Ordering.natural().compare((Comparable) left, (Comparable) right);
	}
}
