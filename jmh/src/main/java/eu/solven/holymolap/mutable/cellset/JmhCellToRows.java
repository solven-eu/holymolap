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

import eu.solven.holymolap.mutable.cellset.FibonacciHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.IReadableHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.Object2IntHolyCellToRow;
import eu.solven.holymolap.mutable.cellset.VariableByteHolyCellToRow;
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
public class JmhCellToRows {

	private IReadableHolyCellToRow object = new Object2IntHolyCellToRow();
	private IReadableHolyCellToRow fibonacci = new FibonacciHolyCellToRow();
	private IReadableHolyCellToRow variableByte = new VariableByteHolyCellToRow();

	@Param({ "1", "8", "16", "32" })
	private int size;

	private IntList DATA_FOR_TESTING;

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(JmhCellToRows.class.getSimpleName()).forks(1).build();
		new Runner(opt).run();
	}

	@Setup
	public void setup() {
		int[] ints = IntStream.range(0, size).toArray();
		DATA_FOR_TESTING = new IntArrayList(ints);
	}

	@Benchmark
	public void object(Blackhole bh) {
		bh.consume(object.getRow(DATA_FOR_TESTING));
	}

	@Benchmark
	public void fibonacci(Blackhole bh) {
		bh.consume(fibonacci.getRow(DATA_FOR_TESTING));
	}

	@Benchmark
	public void variableByte(Blackhole bh) {
		bh.consume(variableByte.getRow(DATA_FOR_TESTING));
	}

}