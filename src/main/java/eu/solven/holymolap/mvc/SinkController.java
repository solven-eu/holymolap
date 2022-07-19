package eu.solven.holymolap.mvc;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import eu.solven.holymolap.stable.v1.IAggregationQuery;
import eu.solven.holymolap.stable.v1.IAggregationResult;
import eu.solven.holymolap.stable.v1.IOneShotAggregator;

/**
 * This enables sinking/loading data.
 * 
 * @author Benoit Lacelle
 *
 */
@Controller
public class SinkController {

	final IOneShotAggregator oneShotAggregator;

	public SinkController(IOneShotAggregator oneShotAggregator) {
		this.oneShotAggregator = oneShotAggregator;
	}

	@GetMapping
	public ResponseEntity<IAggregationResult> aggregate(IAggregationQuery query) {
		return ResponseEntity.internalServerError().build();
	}
}
