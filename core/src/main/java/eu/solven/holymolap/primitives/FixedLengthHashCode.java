package eu.solven.holymolap.primitives;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.IntList;

//Inspired by https://richardstartin.github.io/posts/explicit-intent-and-even-faster-hash-codes
public class FixedLengthHashCode {
	private final int[] coefficients;

	public FixedLengthHashCode(int maxLength) {
		this.coefficients = new int[maxLength + 1];
		coefficients[0] = 1;
		for (int i = 1; i <= maxLength; ++i) {
			coefficients[i] = 31 * coefficients[i - 1];
		}
	}

	public int hashCode(int[] ints) {
		final int max = ints.length;

		if (max > coefficients.length) {
			return Arrays.hashCode(ints);
		}

		int result = coefficients[max];
		for (int i = 0; i < ints.length && i < coefficients.length - 1; ++i) {
			result += coefficients[max - i - 1] * ints[i];
		}
		return result;
	}

	public int hashCode(IntList ints) {
		final int max = ints.size();

		if (max > coefficients.length) {
			// Do not rely on ints.hashCode else it may recursively call itself
			return Arrays.hashCode(ints.toIntArray());
		}

		int result = coefficients[max];
		for (int i = 0; i < ints.size() && i < coefficients.length - 1; ++i) {
			result += coefficients[max - i - 1] * ints.getInt(i);
		}
		return result;
	}
}