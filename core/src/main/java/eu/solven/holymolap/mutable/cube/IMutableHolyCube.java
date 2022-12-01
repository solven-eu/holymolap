package eu.solven.holymolap.mutable.cube;

import java.util.stream.Stream;

import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.sink.record.IHolyCubeRecord;
import eu.solven.holymolap.sink.record.IHolyMeasuresRecordsTable;
import eu.solven.holymolap.sink.record.IHolyRecordsTable;

/**
 * A mutable {@link IHolyCube}, accepting new rows, contributing into new or existing cells.
 * 
 * @author Benoit Lacelle
 *
 */
public interface IMutableHolyCube extends ICloseableToHolyCube {

	void acceptRowToCell(Stream<? extends IHolyCubeRecord> toAdd);

	void acceptRowToCell(IHolyRecordsTable cellsToAdd, IHolyMeasuresRecordsTable measuresToAdd);
}
