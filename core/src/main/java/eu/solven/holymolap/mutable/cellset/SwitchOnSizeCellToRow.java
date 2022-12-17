package eu.solven.holymolap.mutable.cellset;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import eu.solven.pepper.memory.IPepperMemoryConstants;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * The motivation of this {@link IAppendableHolyCellToRow} is due to the slowness of some implementation on too many
 * writes (e.g. due to re-hashing). Hence, after some size, we switch to a RAM-friendly implementation.
 * 
 * @author Benoit Lacelle
 *
 */
public class SwitchOnSizeCellToRow implements IAppendableHolyCellToRow {
	protected final int sizeBeforeSwitch;

	protected final IReadableHolyCellToRow smartCellToRow;
	protected final IAppendableHolyCellToRow naiveCellToRow;

	protected final AtomicBoolean isTooBig = new AtomicBoolean(false);

	public SwitchOnSizeCellToRow() {
		// We switch to a naive implementation after 1M different cells
		this(IPepperMemoryConstants.GB_INT);
	}

	public SwitchOnSizeCellToRow(int sizeBeforeSwitch) {
		this(sizeBeforeSwitch, () -> new VariableByteHolyCellToRow());
	}

	public SwitchOnSizeCellToRow(Supplier<IReadableHolyCellToRow> bijectiveSupplier) {
		this(IPepperMemoryConstants.GB_INT, bijectiveSupplier);
	}

	public SwitchOnSizeCellToRow(int sizeBeforeSwitch, Supplier<IReadableHolyCellToRow> bijectiveSupplier) {
		this.sizeBeforeSwitch = sizeBeforeSwitch;
		smartCellToRow = bijectiveSupplier.get();
		naiveCellToRow = new NaiveHolyCellToRow(sizeBeforeSwitch);
	}

	@Override
	public int getMayAppendRow(IntList coordinates) {
		if (isTooBig.get()) {
			// Try finding the coordinate in the smart cellToRow: we spend CPU for smaller cellIndexes
			int smartCoordinate = smartCellToRow.getRow(coordinates);

			if (smartCoordinate >= 0) {
				return smartCoordinate;
			} else {
				return naiveCellToRow.getMayAppendRow(coordinates);
			}
		}

		int cellIndex = smartCellToRow.getMayAppendRow(coordinates);

		if (cellIndex < 0 && size() >= sizeBeforeSwitch) {
			// This is a new cell, and the size is now too big: we switch to the new cellToRow
			// We do not drop the old cellToRow as the cellIndexes has been diffused in order data-structures
			isTooBig.set(true);
		}

		return cellIndex;
	}

	@Override
	public int size() {
		return smartCellToRow.size();
	}

}
