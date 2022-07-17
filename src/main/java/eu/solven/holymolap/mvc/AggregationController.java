package eu.solven.holymolap.mvc;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAggregationResult;
import eu.solven.holymolap.stable.v1.IOneShotAggregator;

@Controller
@RequestMapping("/v1/query")
public class AggregationController {

	final IOneShotAggregator oneShotAggregator;

	public AggregationController(IOneShotAggregator oneShotAggregator) {
		this.oneShotAggregator = oneShotAggregator;
	}

	@GetMapping
	public ResponseEntity<IAggregationResult> aggregate(IAggregationQuery query) {
		return ResponseEntity.ok(oneShotAggregator.aggregate(query));
	}
}
