package eu.solven.holymolap.sink.record.csv;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTableVisitor;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.reading.CsvReader.Result;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class CsvHolyRecordsTable implements IHolyRecordsTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(CsvHolyRecordsTable.class);

	private final List<String> axes;
	private final long numRows;
	private final Result csvResult;
	private final Predicate<String> axisPredicate;

	/**
	 * 
	 * @param axes
	 * @param numRows
	 * @param csvResult
	 * @param axisPredicate
	 *            useful to reject some axis from this {@link IHolyRecordsTable}
	 */
	public CsvHolyRecordsTable(List<String> axes, long numRows, Result csvResult, Predicate<String> axisPredicate) {
		this.axes = axes;
		this.numRows = numRows;
		this.csvResult = csvResult;
		this.axisPredicate = axisPredicate;
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordsTableVisitor visitor) {
		int i = -1;
		for (CsvReader.ResultColumn col : csvResult) {
			i++;

			String axis = col.name();
			if (!axisPredicate.test(axis)) {
				LOGGER.debug("We skip {}", axis);
				continue;
			}

			switch (col.dataType()) {
			case STRING: {
				String[] data = (String[]) col.data();

				visitor.onObject(i, Arrays.asList(data));
				break;
			}
			case INT: {
				int[] data = (int[]) col.data();

				visitor.onObject(i, IntArrayList.wrap(data));
				break;
			}
			case LONG: {
				long[] data = (long[]) col.data();

				visitor.onLong(i, data);
				break;
			}
			case DOUBLE: {
				double[] data = (double[]) col.data();

				visitor.onDouble(i, data);
				break;
			}
			default: {
				LOGGER.warn("We drop column={} due to not-managed type: {}", axis, col.dataType());
			}
			}
		}
	}

	@Override
	public long size() {
		return numRows;
	}
}
