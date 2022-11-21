package eu.solven.holymolap.sink;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import eu.solven.pepper.logging.PepperLogHelper;

public class LoadingContext {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadingContext.class);

	final Meter inserts = new Meter();
	final Meter cells = new Meter();
	final Meter measures = new Meter();

	final String name;

	// https://stackoverflow.com/questions/153724/how-to-round-a-number-to-n-decimal-places-in-java
	private final static DecimalFormat FORMAT_RATE = new DecimalFormat("#.####");
	static {
		FORMAT_RATE.setRoundingMode(RoundingMode.CEILING);
	}

	public LoadingContext(String name) {
		super();
		this.name = name;
	}

	public LoadingContext() {
		this("no_name");
	}

	public void markInsert(long nbInserts) {
		long markBefore = inserts.getCount();
		inserts.mark(nbInserts);
		long markAfter = inserts.getCount();

		// This method may be called a large number of times with n == 1
		if (nbInserts == 1L && Long.bitCount(inserts.getCount()) == 1
				|| nextPowerOf2(markBefore) != nextPowerOf2(markAfter)) {
			logSinkRate();
		}
	}

	// https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	private static long nextPowerOf2(long l) {
		l--;
		l |= l >> 1;
		l |= l >> 2;
		l |= l >> 4;
		l |= l >> 8;
		l |= l >> 16;
		l |= l >> 32;
		l |= l >> 64;
		l++;

		return l;
	}

	public void logSinkRate() {
		// TODO Log only since a new row this previous log
		LOGGER.info("{} sinked {} rows. Loading at {} rows/sec",
				name,
				PepperLogHelper.humanBytes(inserts.getCount()),
				FORMAT_RATE.format(inserts.getMeanRate()));
	}

	public void markNewCell(long nbNewCells) {
		cells.mark(nbNewCells);
		if (Long.bitCount(cells.getCount()) == 1) {
			logCellRate();
		}
	}

	public void logCellRate() {
		LOGGER.info("{} registered {} cells. Loading at {} cells/sec",
				name,
				PepperLogHelper.humanBytes(cells.getCount()),
				FORMAT_RATE.format(cells.getMeanRate()));
	}

	public void markMeasureContributions(long nbContributions) {
		measures.mark(nbContributions);
		if (Long.bitCount(measures.getCount()) == 1) {
			logMeasureRate();
		}
	}

	public void logMeasureRate() {
		LOGGER.info("{} contributed {} measureAggregates. Loading at {} measureAggregates/sec",
				name,
				PepperLogHelper.humanBytes(measures.getCount()),
				FORMAT_RATE.format(measures.getMeanRate()));
	}

}
