package eu.solven.holymolap.sink.csv;

import eu.solven.holymolap.cube.IHolyCube;

public class LoadResult {
	protected final long numRows;

	protected final IHolyCube holyCube;

	public LoadResult(long numRows, IHolyCube holyCube) {
		this.numRows = numRows;
		this.holyCube = holyCube;
	}

	/**
	 * 
	 * @return how many rows were present in the CSV files
	 */
	public long getNumRows() {
		return numRows;
	}

	public IHolyCube getHolyCube() {
		return holyCube;
	}

}
