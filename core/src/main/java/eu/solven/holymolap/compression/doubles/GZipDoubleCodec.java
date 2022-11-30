package eu.solven.holymolap.compression.doubles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipDoubleCodec implements IDoubleCodec {

	@Override
	public void compress(double[] doubles, ByteBuffer buffer) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
			ByteBuffer bb = ByteBuffer.allocate(doubles.length * 8);
			bb.asDoubleBuffer().put(doubles);
			gzip.write(bb.array());
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		buffer.put(baos.toByteArray());
	}

	@Override
	public void uncompress(ByteBuffer buffer, double[] doubles) {
		ByteBuffer bb = ByteBuffer.allocate(doubles.length * 8);

		ByteArrayInputStream baos = new ByteArrayInputStream(bb.array());
		try (GZIPInputStream gzip = new GZIPInputStream(baos)) {
			byte[] doublesBytes = gzip.readAllBytes();
			bb.put(doublesBytes);

			bb.asDoubleBuffer().get(doubles);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

}
