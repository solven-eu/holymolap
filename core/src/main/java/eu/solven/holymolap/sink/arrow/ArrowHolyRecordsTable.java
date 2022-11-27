package eu.solven.holymolap.sink.arrow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solven.holymolap.sink.record.IHolyRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTableVisitor;

public class ArrowHolyRecordsTable implements IHolyRecordsTable {
	private static final Logger LOGGER = LoggerFactory.getLogger(ArrowHolyRecordsTable.class);

	private final List<String> axes;
	private final int numRows;
	private final List<FieldVector> fieldVectors;
	private final Predicate<String> axisPredicate;

	/**
	 * 
	 * @param axes
	 * @param numRows
	 * @param fieldVectors
	 * @param inCell
	 *            useful to reject some axis from this {@link IHolyRecordsTable}
	 */
	public ArrowHolyRecordsTable(List<String> axes,
			int numRows,
			List<FieldVector> fieldVectors,
			Predicate<String> axisPredicate) {
		this.axes = axes;
		this.numRows = numRows;
		this.fieldVectors = fieldVectors;
		this.axisPredicate = axisPredicate;
	}

	@Override
	public List<String> getAxes() {
		return axes;
	}

	@Override
	public void accept(IHolyRecordsTableVisitor visitor) {
		int colIndex = -1;

		// PERF: Rely on ValueHolder
		for (FieldVector col : fieldVectors) {
			colIndex++;

			String axis = col.getName();
			if (!axisPredicate.test(axis)) {
				LOGGER.debug("We skip {}", axis);
				continue;
			}

			switch (col.getField().getType().getTypeID()) {
			case Null: {
				break;
			}
			case Int: {
				// TODO No need to allocate a long[]: we may provide a LongList, proxy to Arrow buffer
				long[] data = new long[numRows];

				BaseIntVector baseIntVector = (BaseIntVector) col;
				for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
					if (!baseIntVector.isNull(rowIndex)) {
						// TODO We should not leave 0 as null
						data[rowIndex] = baseIntVector.getValueAsLong(rowIndex);
					}
				}

				visitor.onLong(colIndex, data);
				break;
			}
			case FloatingPoint: {
				double[] data = new double[numRows];

				FloatingPointVector floatingPointVector = (FloatingPointVector) col;
				for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
					if (!floatingPointVector.isNull(rowIndex)) {
						data[rowIndex] = floatingPointVector.getValueAsDouble(rowIndex);
					}
				}

				visitor.onDouble(colIndex, data);
				break;
			}
			case Utf8: {
				List<Object> data = new ArrayList<>(numRows);

				for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
					// From Text to String
					Object text = col.getObject(rowIndex);
					if (text != null) {
						data.add(text.toString());
					}
				}

				visitor.onObject(colIndex, data);
				break;
			}
			default: {
				LOGGER.warn("We drop column={} due to not-managed type: {}",
						axis,
						col.getField().getType().getTypeID());
			}
			}
		}
	}

	@Override
	public long size() {
		return numRows;
	}
}
