package eu.solven.holymolap.cube.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Some {@link IHolyCellSet} enables lazy indexation. It means some data-structures can be lazily built to improve
 * the performance of some queries.
 * 
 * @author Benoit Lacelle
 *
 */
public interface ILazyHolyCellSet extends IHolyCellSet {

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
			int axisIndex = getAxisIndex(axis);

			futures.add(startIndexing(axisIndex));
		}

		return Futures.allAsList(futures);
	}

}
