package eu.solven.holymolap.mvc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListeningExecutorService;

import eu.solven.holymolap.cube.composite.CompositeHolyCube;
import eu.solven.holymolap.cube.composite.ICompositeHolyCube;
import eu.solven.holymolap.sink.csv.LoadFromCsv;
import eu.solven.holymolap.sink.csv.LoadResult;
import eu.solven.holymolap.stable.beta.IAggregationResult;
import eu.solven.pepper.thread.PepperExecutorsHelper;
import io.deephaven.csv.util.CsvReaderException;

/**
 * This enables sinking/loading data.
 * 
 * @author Benoit Lacelle
 *
 */
@Controller
public class SinkController {
	final ListeningExecutorService les = PepperExecutorsHelper.newShrinkableCachedThreadPool("mvc.Load",
			new ThreadPoolExecutor.DiscardOldestPolicy());

	final LoadingCache<LoadingRequest, ICompositeHolyCube> requestToCube = CacheBuilder.newBuilder()
			.expireAfterAccess(1, TimeUnit.HOURS)
			.build(CacheLoader
					.asyncReloading(CacheLoader.from(r -> new CompositeHolyCube(executeLoad(r).getHolyCube())), les));

	public ICompositeHolyCube getIfPresent(LoadingRequest request) {
		return requestToCube.getIfPresent(request);
	}

	@GetMapping
	public ResponseEntity<IAggregationResult> registerLoad(LoadingRequest query) {
		requestToCube.refresh(query);

		return ResponseEntity.accepted().build();
	}

	private LoadResult executeLoad(LoadingRequest loadingRequest) {
		String url = loadingRequest.getUrl();
		if (!url.startsWith("file://") || !url.endsWith(".csv")) {
			throw new IllegalArgumentException(
					"Invalid URL: " + url + " (we accept only local single csv file for now)");
		}

		try {
			return makeLoadFromCsv().loadSingleCsvFile(parseUrl(url));
		} catch (CsvReaderException | IOException e) {
			throw new IllegalArgumentException("Issue with url=" + url, e);
		}
	}

	private File parseUrl(String url) {
		return new File(url.substring("file://".length()));
	}

	protected LoadFromCsv makeLoadFromCsv() {
		return new LoadFromCsv();
	}
}
