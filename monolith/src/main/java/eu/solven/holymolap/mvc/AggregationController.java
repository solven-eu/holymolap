package eu.solven.holymolap.mvc;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.solven.holymolap.stable.beta.MapAggregationResult;
import eu.solven.holymolap.stable.beta.IAggregationResult;
import eu.solven.holymolap.stable.beta.IOneShotAggregator;
import eu.solven.holymolap.stable.v1.IAggregationQuery;

@RestController
@RequestMapping("/v1/query")
public class AggregationController {

	final SinkController sinkController;
	final IOneShotAggregator oneShotAggregator;

	public AggregationController(SinkController sinkController, IOneShotAggregator oneShotAggregator) {
		this.sinkController = sinkController;
		this.oneShotAggregator = oneShotAggregator;
	}

	@GetMapping
	public IAggregationResult aggregate(LoadingRequest loadingRequest, IAggregationQuery query) {
		return new MapAggregationResult(oneShotAggregator.aggregate(sinkController.getIfPresent(loadingRequest), query));
	}
}
