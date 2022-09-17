package eu.solven.holymolap.cube.cellset;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Some {@link IHolyCellMultiSet} enables lazy indexation. It means some data-structures can be lazily built to improve
 * the performance of some queries.
 * 
 * @author Benoit Lacelle
 *
 */
@Deprecated
public interface ILazyHolyCellMultiSet extends IHolyCellMultiSet {

	/**
	 * 
	 * @param axisIndex
	 * @return A {@link ListenableFuture} tracking the indexation of given axis
	 */
	ListenableFuture<?> startIndexing(int axisIndex);

	/**
	 * 
	 * @param axisIndex
	 * @return A {@link ListenableFuture} tracking the indexation of given axes
	 */
	default ListenableFuture<? extends List<?>> startIndexing(Set<String> axes) {
		List<ListenableFuture<?>> futures = new ArrayList<>();

		for (String axis : axes) {
			int axisIndex = getAxesWithCoordinates().getAxisIndex(axis);

			futures.add(startIndexing(axisIndex));
		}

		return Futures.allAsList(futures);
	}

}
