package eu.solven.holymolap.mutable.cellset;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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

import eu.solven.holymolap.compression.doubles.DictionaryDoubleList;
import eu.solven.holymolap.compression.doubles.FcpDoubleColumn;
import eu.solven.holymolap.mutable.cellset.FibonacciHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.IHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.Object2IntHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.VariableByteHolyCellToRow;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

// https://www.oracle.com/technical-resources/articles/java/architect-benchmarking.html
// https://stackoverflow.com/questions/38056899/jmh-unable-to-find-the-resource-meta-inf-benchmarklist
// before running main: mvn clean package -DskipTests
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms2G", "-Xmx2G" })
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
public class JmhDoubleArrayCompression {

	private DoubleList array;
	private DoubleList dictionary;
	private DoubleList variableByte;

	@Param({ "1", "8", "16", "32" })
	private int size;

	private double[] DATA_FOR_TESTING;

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(JmhDoubleArrayCompression.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	@Setup
	public void setup() {
		DATA_FOR_TESTING = IntStream.range(0, size).mapToDouble(i -> (double) i).toArray();

		// Used to benchmark reads
		array = new DoubleArrayList(DATA_FOR_TESTING);
		dictionary = new DictionaryDoubleList(DATA_FOR_TESTING);
		variableByte = new FcpDoubleColumn(DATA_FOR_TESTING);
	}

	@Benchmark
	public void makeObject(Blackhole bh) {
		bh.consume(new DoubleArrayList(DATA_FOR_TESTING));
	}

	@Benchmark
	public void makeDictionary(Blackhole bh) {
		bh.consume(new DictionaryDoubleList(DATA_FOR_TESTING));
	}

	@Benchmark
	public void makeReOrderVariableByte(Blackhole bh) {
		bh.consume(new FcpDoubleColumn(DATA_FOR_TESTING));
	}

	@Benchmark
	public void readObject(Blackhole bh) {
		bh.consume(array.toDoubleArray());
	}

	@Benchmark
	public void readDictionary(Blackhole bh) {
		bh.consume(dictionary.toDoubleArray());
	}

	@Benchmark
	public void readReOrderVariableByte(Blackhole bh) {
		bh.consume(variableByte.toDoubleArray());
	}

}