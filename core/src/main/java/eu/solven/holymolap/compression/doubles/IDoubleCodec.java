package eu.solven.holymolap.compression.doubles;

import java.nio.ByteBuffer;

/**
 * Encode double[] into BytBuffer
 * 
 * @author Benoit Lacelle
 *
 */
public interface IDoubleCodec {

	void compress(double[] doubles, ByteBuffer buffer);

	void uncompress(ByteBuffer buffer, double[] doubles);

}
