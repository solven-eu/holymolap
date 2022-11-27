package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

import com.github.kutschkem.fpc.FpcCompressor;

/**
 * Encode double[] into BytBuffer
 * 
 * @author Benoit Lacelle
 *
 */
public class FcpDoubleCodec implements IDoubleCodec {
	final FpcCompressor fpc = new FpcCompressor();

	@Override
	public void compress(double[] doubles, ByteBuffer buffer) {
		fpc.compress(buffer, doubles);
	}

	@Override
	public void uncompress(ByteBuffer buffer, double[] doubles) {
		fpc.decompress(buffer, doubles);
	}
}
