package eu.solven.holymolap.mutable.cellset;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;

// 0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144
public class TestFibonacciEncoding {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestFibonacciEncoding.class);

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

	@Test
	public void testFibonacciEncodingToLong_NearMax() {
		Assertions
				.assertThat(longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(0, IntArrayList.of(Integer.MAX_VALUE - 1))))
				.isEqualTo("0001010001000101001000001001000100001000100011100000000000000000");

		// 0 is not encodable by Fibonacci. Hence, all ints are +1, hence Integer.MAX_VALUE is disallowed
		Assertions
				.assertThatThrownBy(() -> longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(0, IntArrayList.of(Integer.MAX_VALUE))))
				.isInstanceOf(IllegalArgumentException.class);

		// Some application would require accepting -1 as a valid input. Then, hence Integer.MAX_VALUE - 1 is disallowed
		Assertions
				.assertThatThrownBy(() -> longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(-1, IntArrayList.of(Integer.MAX_VALUE - 1))))
				.isInstanceOf(IllegalArgumentException.class);

		Assertions
				.assertThat(longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(0, IntArrayList.of(Integer.MAX_VALUE - 1))))
				.isEqualTo(longToBinaryWithLeading(
						FibonacciEncoding.fibonacciEncodingToLong(-1, IntArrayList.of(Integer.MAX_VALUE - 2))));
	}

	@Test
	public void testFibonacciEncodingToLong_Overflow() {
		// We check a single zero is encoded as 2 bytes.
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(0, IntArrayList.of(0))))
				.matches("11" + "0{62}");

		// We check a two zeros is encoded as 4 bytes.
		Assertions
				.assertThat(
						longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(0, IntArrayList.of(0, 0))))
				.matches("1111" + "0{60}");

		// We check a 32 zero is encoded as 64 bytes.
		Assertions.assertThat(longToBinaryWithLeading(FibonacciEncoding.fibonacciEncodingToLong(0,
				IntArrayList.of(IntStream.range(0, 32).map(i -> 0).toArray())))).matches("1{64}");

		// We check an overflow throws: append a 0 (2 bits) after 64 bits being consumed
		Assertions
				.assertThatThrownBy(() -> FibonacciEncoding.fibonacciEncodingToLong(0,
						IntArrayList.of(IntStream.range(0, 32 + 1).map(i -> 0).toArray())))
				.isInstanceOf(IllegalArgumentException.class);

		// We check an overflow throws: append a 1 (3 bits) after 62 bits being consumed
		Assertions.assertThatThrownBy(() -> {
			IntArrayList thirtyOneZeros = IntArrayList.of(IntStream.range(0, 32 - 1).map(i -> 0).toArray());
			thirtyOneZeros.add(1);
			FibonacciEncoding.fibonacciEncodingToLong(0, thirtyOneZeros);
		}).isInstanceOf(IllegalArgumentException.class);
	}

	private String longToBinaryWithLeading(long longs) {
		return FibonacciEncoding.longToBinaryWithLeading(longs);
	}

	@Test
	public void testVariableByte_LargeSingleInput() {
		VariableByte codec = new VariableByte();

		{
			byte[] output = new byte[5];
			codec.compress(new int[] { Integer.MAX_VALUE }, new IntWrapper(), 1, output, new IntWrapper());

			Assertions.assertThat(output).containsExactly(127, 127, 127, 127, -121);
		}

		{
			int[] output = new int[2];
			codec.compress(new int[] { Integer.MAX_VALUE }, new IntWrapper(), 1, output, new IntWrapper());

			// Assertions.assertThat(output).containsExactly(Integer.MAX_VALUE, 135);
		}
	}

	/**
	 * Given b='11110000', isBit(b, [0,3]) returns true, while isBit(b, [4,7]) returns false
	 * 
	 * @param b
	 * @param positionFromLeft
	 * @return true if the bit at index positionFromLeft is 1
	 */
	public boolean isBit(byte b, int positionFromLeft) {
		if (positionFromLeft < 0 || positionFromLeft >= 8) {
			throw new IllegalArgumentException("position=" + positionFromLeft);
		}

		byte singleBit = FibonacciEncodingCodec.rotateRight((byte) 1, positionFromLeft + 1);
		return singleBit == (singleBit & b);
	}

	private String fibonacciEncode(FibonacciEncodingCodec codec, int... inputs) {
		byte[] output = new byte[8];
		IntWrapper outputPosition = new IntWrapper();
		codec.compress(inputs, new IntWrapper(), inputs.length, output, outputPosition);

		{
			byte lastByte = output[outputPosition.get() - 1];
			String lastByteAsString = FibonacciEncoding.byteToBinaryWithLeading(lastByte);

			if (lastByte == (byte) 0) {
				throw new IllegalStateException("The last byte can not hold only 0 withFibonacciEncoding");
			}

			int lastBitSet;
			for (lastBitSet = 7; lastBitSet >= 0; lastBitSet--) {
				if (isBit(lastByte, lastBitSet)) {
					break;
				}
			}

			byte beforeLastByte = output.length >= 2 ? (byte) 0 : output[outputPosition.get() - 2];
			if (isBit(lastByte, lastBitSet) && lastBitSet >= 1 ? isBit(lastByte, lastBitSet - 1)
					: isBit(beforeLastByte, 8 + lastBitSet - 1) && lastBitSet >= 2 ? !isBit(lastByte, lastBitSet - 2)
							: isBit(beforeLastByte, 8 + lastBitSet - 1)) {
				LOGGER.debug("OK: {}{}", FibonacciEncoding.byteToBinaryWithLeading(beforeLastByte), lastByteAsString);
			} else {
				throw new IllegalArgumentException("KO: " + FibonacciEncoding.byteToBinaryWithLeading(output));
			}

		}

		long firstLong = ByteBuffer.wrap(output).asLongBuffer().get();
		return longToBinaryWithLeading(firstLong);
	}

	/**
	 * Like fibonacciEncode, but we start from a byte[] full of 1, to ensure we properly write zeros
	 * 
	 * @param codec
	 * @param inputs
	 * @return
	 */
	private String fibonacciEncode_adverse(FibonacciEncodingCodec codec, int... inputs) {
		byte[] output = new byte[8];

		for (int i = 0; i < output.length; i++) {
			output[i] = -1;
		}

		codec.compress(inputs, new IntWrapper(), inputs.length, output, new IntWrapper());

		long firstLong = ByteBuffer.wrap(output).asLongBuffer().get();
		return longToBinaryWithLeading(firstLong);
	}

	@Test
	public void testFibonacciCodec_singleInt() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		Assertions.assertThat(fibonacciEncode(codec, 1)).matches("11" + "0{62}");

		Assertions.assertThat(fibonacciEncode(codec, 2)).matches("011" + "0{61}");
		Assertions.assertThat(fibonacciEncode(codec, 3)).matches("0011" + "0{60}");
		Assertions.assertThat(fibonacciEncode(codec, 4)).matches("1011" + "0{60}");
		Assertions.assertThat(fibonacciEncode(codec, 5)).matches("00011" + "0{59}");
		Assertions.assertThat(fibonacciEncode(codec, 6)).matches("10011" + "0{59}");
		Assertions.assertThat(fibonacciEncode(codec, 7)).matches("01011" + "0{59}");
		Assertions.assertThat(fibonacciEncode(codec, 8)).matches("000011" + "0{58}");
		Assertions.assertThat(fibonacciEncode(codec, 9)).matches("100011" + "0{58}");
		Assertions.assertThat(fibonacciEncode(codec, 10)).matches("010011" + "0{58}");

		Assertions.assertThat(fibonacciEncode(codec, 143)).matches("01010101011" + "0{53}");
	}

	@Test
	public void testFibonacciCodec_singleInt_adverse() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		Assertions.assertThat(fibonacciEncode_adverse(codec, 1)).matches("11" + "000000" + "1{56}");

		Assertions.assertThat(fibonacciEncode_adverse(codec, 2)).matches("011" + "00000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 3)).matches("0011" + "0000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 4)).matches("1011" + "0000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 5)).matches("00011" + "000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 6)).matches("10011" + "000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 7)).matches("01011" + "000" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 8)).matches("000011" + "00" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 9)).matches("100011" + "00" + "1{56}");
		Assertions.assertThat(fibonacciEncode_adverse(codec, 10)).matches("010011" + "00" + "1{56}");

		Assertions.assertThat(fibonacciEncode_adverse(codec, 143)).matches("01010101011" + "00000" + "1{48}");
	}

	@Test
	public void testFibonacciCodec_MultipleInts() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		Assertions.assertThat(fibonacciEncode(codec, 1, 1)).matches("1111" + "0{60}");

		Assertions.assertThat(fibonacciEncode(codec, 5, 1, 10, 143))
				.matches("00011" + "11" + "010011" + "01010101011" + "0{40}");
	}

	@Test
	public void testFibonacciCodec_MultipleInts_adverse() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		Assertions.assertThat(fibonacciEncode_adverse(codec, 1, 1)).matches("1111" + "0000" + "1{56}");

		Assertions.assertThat(fibonacciEncode_adverse(codec, 5, 1, 10, 143))
				.matches("00011" + "11" + "010011" + "01010101011" + "1{40}");
	}

	@Test
	public void testFibonacciCodec_CheckPositions_singleIntSingleByte() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		byte[] output = new byte[1];
		IntWrapper inputPosition = new IntWrapper();
		IntWrapper outputPosition = new IntWrapper();

		int[] input = new int[] { 1 };
		codec.compress(input, inputPosition, input.length, output, outputPosition);

		Assertions.assertThat(inputPosition.get()).isEqualTo(1);
		Assertions.assertThat(outputPosition.get()).isEqualTo(1);
	}

	@Test
	public void testFibonacciCodec_CheckPositions_singleIntMultipleByte() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		byte[] output = new byte[2];
		IntWrapper inputPosition = new IntWrapper();
		IntWrapper outputPosition = new IntWrapper();

		int[] input = new int[] { 143 };
		codec.compress(input, inputPosition, input.length, output, outputPosition);

		Assertions.assertThat(inputPosition.get()).isEqualTo(1);
		Assertions.assertThat(outputPosition.get()).isEqualTo(2);
	}

	@Test
	public void testFibonacciCodec_CheckPositions_multipleIntSingleByte() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		byte[] output = new byte[1];
		IntWrapper inputPosition = new IntWrapper();
		IntWrapper outputPosition = new IntWrapper();

		int[] input = new int[] { 1, 2 };
		codec.compress(input, inputPosition, input.length, output, outputPosition);

		Assertions.assertThat(inputPosition.get()).isEqualTo(2);
		Assertions.assertThat(outputPosition.get()).isEqualTo(1);
	}

	@Test
	public void testFibonacciCodec_CheckPositions_multipleIntMultipleByte() {
		FibonacciEncodingCodec codec = new FibonacciEncodingCodec(1);

		byte[] output = new byte[3];
		IntWrapper inputPosition = new IntWrapper();
		IntWrapper outputPosition = new IntWrapper();

		int[] input = new int[] { 5, 1, 10, 143 };
		codec.compress(input, inputPosition, input.length, output, outputPosition);

		Assertions.assertThat(inputPosition.get()).isEqualTo(4);
		Assertions.assertThat(outputPosition.get()).isEqualTo(3);
	}

}
