package eu.solven.holymolap.compression.doubles;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import it.unimi.dsi.fastutil.doubles.DoubleList;

/**
 * A {@link DoubleList} based on {@link ZstdCompressor}
 * 
 * @author Benoit Lacelle
 *
 */
public class ZstdDoubleList extends ACompressorDoubleList {

	public ZstdDoubleList(double[] array) {
		super(new ZstdCompressor(), new ZstdDecompressor(), array);
	}
}
