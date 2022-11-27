package eu.solven.holymolap.mutable.cellset;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import eu.solven.holymolap.mutable.cellset.FibonacciEncoding;
import eu.solven.holymolap.mutable.cellset.FibonacciEncodingCodec;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import me.lemire.integercompression.IntWrapper;

// https://www.oracle.com/technical-resources/articles/java/architect-benchmarking.html
// https://stackoverflow.com/questions/38056899/jmh-unable-to-find-the-resource-meta-inf-benchmarklist
// before running main: mvn clean package -DskipTests
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2G", "-Xmx2G" })
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
public class JmhFibonacciEncoding {

	private FibonacciEncodingCodec codec = new FibonacciEncodingCodec(0);

	@Param({ "1", "123", "123456" })
	private int first;

	@Param({ "0", "1", "123", "123456" })
	private int second;

	private IntList DATA_FOR_TESTING;

	final IntWrapper inPosition = new IntWrapper();
	final IntWrapper outPosition = new IntWrapper();
	final byte[] output = new byte[8];

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(JmhFibonacciEncoding.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	@Setup
	public void setup() {
		DATA_FOR_TESTING = createData();
	}

	private IntList createData() {
		IntList intList = new IntArrayList();

		intList.add(first);
		if (second != 0) {
			intList.add(first + second);
		}

		return intList;
	}

	@Benchmark
	public void encodeToLong(Blackhole bh) {
		bh.consume(FibonacciEncoding.fibonacciEncodingToLong(0, DATA_FOR_TESTING));
	}

	@Benchmark
	public void encodeToBytes(Blackhole bh) {
		inPosition.set(0);
		outPosition.set(0);

		codec.compress(DATA_FOR_TESTING, inPosition, DATA_FOR_TESTING.size(), output, outPosition);

		bh.consume(output);
	}

}