package eu.solven.holymolap;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class RowsConsumerStatus {

	protected final Date startDate;
	protected final long nbToConsider;

	protected final AtomicLong nbConsidered = new AtomicLong();

	public RowsConsumerStatus(long nbToConsider) {
		this(nbToConsider, new Date());
	}

	public RowsConsumerStatus(long nbToConsider, Date startDate) {
		this.startDate = startDate;
		this.nbToConsider = nbToConsider;
	}

	public long getNbToConsider() {
		return nbToConsider;
	}

	public boolean isComplete() {
		return nbToConsider == nbConsidered.get();
	}

	public long addAsConsidered(int cardinality) {
		return nbConsidered.addAndGet(cardinality);
	}

	@Override
	public String toString() {
		return "RowsConsumerStatus [startDate=" + startDate + ", nbToConsider=" + nbToConsider + ", nbConsidered=" + nbConsidered + "]";
	}

}
