package eu.solven.holymolap.mutable.cellset;

import org.assertj.core.api.Assertions;
import org.junit.Test;

// 0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144
public class TestFibonacciEncoding {
	@Test
	public void testFibonacci() {
		Assertions.assertThatThrownBy(() -> FibonacciEncoding.largestFiboLessOrEqual(0))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(1)).isEqualTo(1);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(2)).isEqualTo(2);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(3)).isEqualTo(3);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(4)).isEqualTo(3);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(5)).isEqualTo(5);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(6)).isEqualTo(5);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(7)).isEqualTo(5);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(8)).isEqualTo(8);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(9)).isEqualTo(8);
		Assertions.assertThat(FibonacciEncoding.largestFiboLessOrEqual(10)).isEqualTo(8);
	}

	@Test
	public void testFibonacciEncoding() {
		Assertions.assertThatThrownBy(() -> FibonacciEncoding.fibonacciEncoding(0))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(1)).isEqualTo("11");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(2)).isEqualTo("011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(3)).isEqualTo("0011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(4)).isEqualTo("1011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(5)).isEqualTo("00011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(6)).isEqualTo("10011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(7)).isEqualTo("01011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(8)).isEqualTo("000011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(9)).isEqualTo("100011");
		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(10)).isEqualTo("010011");

		Assertions.assertThat(FibonacciEncoding.fibonacciEncoding(143)).isEqualTo("01010101011");
	}

	@Test
	public void testFibonacciEncodingToLong() {
		Assertions.assertThatThrownBy(() -> FibonacciEncoding.fibonacciEncodingToLong(0))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(1)))
				.matches("11" + "0{62}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(2)))
				.matches("011" + "0{61}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(3)))
				.matches("0011" + "0{60}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(4)))
				.matches("1011" + "0{60}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(5)))
				.matches("00011" + "0{59}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(6)))
				.matches("10011" + "0{59}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(7)))
				.matches("01011" + "0{59}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(8)))
				.matches("000011" + "0{58}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(9)))
				.matches("100011" + "0{58}");
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(10)))
				.matches("010011" + "0{58}");

		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(143)))
				.matches("01010101011" + "0{53}");
	}

	@Test
	public void testFibonacciEncodingToLong_fromArrayUnsignedInt() {
		Assertions
				.assertThat(longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(new int[] { 5 - 1, 0, 10 - 1, 143 - 1 })))
				.matches("00011" + "11" + "010011" + "01010101011" + "0{40}");
	}

	private String longToBinaryWithLeading(long longs) {
		return FibonacciEncoding.longToBinaryWithLeading(longs);
	}

}
