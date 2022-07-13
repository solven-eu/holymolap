package eu.solven.holymolap.sink;

import java.util.Iterator;

import eu.solven.holymolap.IHolyCube;

public interface IRoaringSink {
	IHolyCube sink(IFastEntry toAdd, ISinkContext context);

	IHolyCube sink(Iterable<? extends IFastEntry> toAdd, ISinkContext context);

	IHolyCube sink(Iterator<? extends IFastEntry> toAdd, ISinkContext context);

}
