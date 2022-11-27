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

	/**
	 * Compress data from an array to another array.
	 * 
	 * Both inpos and outpos are modified to represent how much data was read and written to. If 12 ints (inlength = 12)
	 * are compressed to 3 ints, then inpos will be incremented by 12 while outpos will be incremented by 3. We use
	 * IntWrapper to pass the values by reference.
	 * 
	 * @param in
	 *            input array
	 * @param inpos
	 *            where to start reading in the array
	 * @param inlength
	 *            how many doubles to compress
	 * @param out
	 *            output array
	 * @param outpos
	 *            where to write in the output array
	 */
	// void compress(double[] in, IntWrapper inpos, int inlength, double[] out, IntWrapper outpos);

	/**
	 * Uncompress data from an array to another array.
	 * 
	 * Both inpos and outpos parameters are modified to indicate new positions after read/write.
	 * 
	 * @param in
	 *            array containing data in compressed form
	 * @param inpos
	 *            where to start reading in the array
	 * @param inlength
	 *            length of the compressed data (ignored by some schemes)
	 * @param out
	 *            where to start writing the uncompressed output in out
	 * @param outpos
	 *            where to write the compressed output in out
	 */
	// void uncompress(double[] in, IntWrapper inpos, int inlength, double[] out, IntWrapper outpos);
}
