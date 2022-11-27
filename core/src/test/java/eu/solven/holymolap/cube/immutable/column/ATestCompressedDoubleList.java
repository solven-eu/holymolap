package eu.solven.holymolap.cube.immutable.column;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.pepper.memory.IPepperMemoryConstants;
import it.unimi.dsi.fastutil.doubles.DoubleList;

public abstract class ATestCompressedDoubleList {
	private static final Logger LOGGER = LoggerFactory.getLogger(ATestCompressedDoubleList.class);

	public abstract Class<? extends DoubleList> getClazz();

	protected DoubleList makeInstance(double[] array) {
		try {
			return getClazz().getConstructor(double[].class).newInstance(array);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException("ARG", e);
		}
	}

	@Test
	public void testCompressDoubleArray_smallDistinct() {
		double[] array =
				IntStream.range(0, 1024).mapToDouble(i -> 0 == i % 2 ? 123456.987654D : 987654.123456D).toArray();

		long nbDistincts = DoubleStream.of(array).distinct().limit(array.length / 16).count();
		Assertions.assertThat(nbDistincts).isEqualTo(2);

		GraphLayout graph = GraphLayout.parseInstance(makeInstance(array));
		Assertions.assertThat(graph.totalSize())
				.isBetween(expectedHeapConsuptionMin_lowDistinct(), expectedHeapConsuptionMax_lowDistinct());

		LOGGER.info("Graph.footprint: {}", graph.toFootprint());
	}

	protected abstract long expectedHeapConsuptionMin_lowDistinct();

	protected abstract long expectedHeapConsuptionMax_lowDistinct();

	@Test
	public void testCompressDoubleArray_aroundOne() {
		Random r = new Random(123);
		double[] array = IntStream.range(0, 1024).mapToDouble(i -> 1D + r.nextDouble()).toArray();

		long nbDistincts = DoubleStream.of(array).distinct().limit(array.length / 16).count();
		Assertions.assertThat(nbDistincts).isEqualTo(64L);

		GraphLayout graph = GraphLayout.parseInstance(makeInstance(array));
		Assertions.assertThat(graph.totalSize())
				.isBetween(expectedHeapConsuptionMin_aroundOne(), expectedHeapConsuptionMax_aroundOne());

		LOGGER.info("Graph.footprint: {}", graph.toFootprint());
	}

	protected abstract long expectedHeapConsuptionMin_aroundOne();

	protected abstract long expectedHeapConsuptionMax_aroundOne();

	@Test
	public void testCompressDoubleArray_wereFloats() {
		Random r = new Random(123);
		double[] array =
				IntStream.range(0, 1024).mapToDouble(i -> r.nextFloat(IPepperMemoryConstants.MB_INT)).toArray();

		long nbDistincts = DoubleStream.of(array).distinct().limit(array.length / 16).count();
		Assertions.assertThat(nbDistincts).isEqualTo(64L);

		GraphLayout graph = GraphLayout.parseInstance(makeInstance(array));
		Assertions.assertThat(graph.totalSize())
				.isBetween(expectedHeapConsuptionMin_wereFloats(), expectedHeapConsuptionMax_wereFloats());

		LOGGER.info("Graph.footprint: {}", graph.toFootprint());
	}

	protected abstract long expectedHeapConsuptionMin_wereFloats();

	protected abstract long expectedHeapConsuptionMax_wereFloats();

	@Test
	public void testCompressDoubleArray_positiveInts() {
		double[] array = IntStream.range(0, 1024).mapToDouble(i -> i).toArray();

		long nbDistincts = DoubleStream.of(array).distinct().limit(array.length / 16).count();
		Assertions.assertThat(nbDistincts).isEqualTo(64L);

		GraphLayout graph = GraphLayout.parseInstance(makeInstance(array));
		Assertions.assertThat(graph.totalSize())
				.isBetween(expectedHeapConsuptionMin_positiveInts(), expectedHeapConsuptionMax_positiveInts());

		LOGGER.info("Graph.footprint: {}", graph.toFootprint());
	}

	protected abstract long expectedHeapConsuptionMin_positiveInts();

	protected abstract long expectedHeapConsuptionMax_positiveInts();
}
