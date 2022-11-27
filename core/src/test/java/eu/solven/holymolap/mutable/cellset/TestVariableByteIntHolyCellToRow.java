package eu.solven.holymolap.mutable.cellset;

import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.openjdk.jol.info.GraphLayout;
import org.quickperf.junit4.QuickPerfJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;

@RunWith(QuickPerfJUnitRunner.class)
public class TestVariableByteIntHolyCellToRow extends ATestHolyCellToRow {
	private static final Logger LOGGER = LoggerFactory.getLogger(TestVariableByteIntHolyCellToRow.class);

	@Override
	protected IHolyCellToRow makeCellToRow() {
		return new VariableByteHolyCellToRow();
	}

	@Override
	protected long expectedHeapConsuptionMin() {
		return 88_000L;
	}

	@Override
	protected long expectedHeapConsuptionMax() {
		return 89_000L;
	}

	// This demonstrates byte[] of length 1+N*8->(N+1)*8 has the same footprint
	@Test
	public void testMemoryByteArrays() {
		for (int i = 0; i < 32; i++) {
			LOGGER.info(i + ": " + GraphLayout.parseInstance(new byte[i]).totalSize());
		}

		// The following relates with -XX:ObjectAlignmentInBytes
		// https://lemire.me/blog/2022/11/22/what-is-the-size-of-a-byte-array-in-java/
		LOGGER.info("long as byte[8]: " + GraphLayout.parseInstance(new byte[8]).totalSize());
		LOGGER.info("long as long[1]: " + GraphLayout.parseInstance(new long[1]).totalSize());

		LOGGER.info("long as byte[9]: " + GraphLayout.parseInstance(new byte[9]).totalSize());
		LOGGER.info("long as long[2]: " + GraphLayout.parseInstance(new long[2]).totalSize());
	}

	// This demonstrates int[] of length 1+N*2->(N+1)*2 has the same footprint
	@Test
	public void testMemoryIntArrays() {
		for (int i = 0; i < 32; i++) {
			LOGGER.info(i + ": " + GraphLayout.parseInstance(new int[i]).totalSize());
		}
	}

	@Test
	public void testVariablebyteVsFibonacci() {
		ByteIntegerCODEC fibonacci = new FibonacciEncodingCodec(0);
		ByteIntegerCODEC variableByte = new VariableByte();

		byte[] buffer = new byte[2048];
		for (int i = 0; i < 1024; i++) {
			int[] in = IntStream.range(0, i + 1).toArray();

			int outPositionFibonacci = compressAndReturnPosition(fibonacci, buffer, in);
			int outPositionVariableByte = compressAndReturnPosition(variableByte, buffer, in);

			LOGGER.info(i + ": "
					+ outPositionFibonacci
					+ " vs "
					+ outPositionVariableByte
					+ " "
					+ (outPositionFibonacci <= outPositionVariableByte ? "F" : "V"));
		}

		for (int i = Integer.MAX_VALUE - 3; i < Integer.MAX_VALUE - 1; i++) {
			int[] in = IntStream.range(i - 2, i).toArray();

			int outPositionFibonacci = compressAndReturnPosition(fibonacci, buffer, in);
			int outPositionVariableByte = compressAndReturnPosition(variableByte, buffer, in);

			LOGGER.info(i + ": "
					+ outPositionFibonacci
					+ " vs "
					+ outPositionVariableByte
					+ " "
					+ (outPositionFibonacci <= outPositionVariableByte ? "F" : "V"));
		}
	}

	private int compressAndReturnPosition(ByteIntegerCODEC codec, byte[] buffer, int[] in) {
		IntWrapper posFibonacci = new IntWrapper(0);

		{
			for (int byteIndex = 0; byteIndex < buffer.length; byteIndex++) {
				buffer[byteIndex] = 0;
			}
			codec.compress(in, new IntWrapper(0), in.length, buffer, posFibonacci);
		}

		int outPositionFibonacci = posFibonacci.get();
		return outPositionFibonacci;
	}
}
