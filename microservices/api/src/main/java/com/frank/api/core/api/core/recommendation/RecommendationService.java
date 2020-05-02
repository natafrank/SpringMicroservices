package com.frank.api.core.api.core.recommendation;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import reactor.core.publisher.Flux;

public interface RecommendationService {

	/**
     * Sample usage:
     *
     * curl -X POST $HOST:$PORT/recommendation \
     *   -H "Content-Type: application/json" --data \
     *   '{"productId":123,"recommendationId":456,"author":"me","rate":5,"content":"yada, yada, yada"}'
     *
     * @param body
     * @return
     */
    @PostMapping(
        value    = "/recommendation",
        consumes = "application/json",
        produces = "application/json")
    Recommendation createRecommendation(@RequestBody Recommendation body);
	
    /**
     * Sample usage: curl $HOST:$PORT/recommendation?productId=1
     *
     * @param productId
     * @return
     */
    @GetMapping(
        value    = "/recommendation",
        produces = "application/json")
    Flux<Recommendation> getRecommendations(@RequestParam(value = "productId", required = true) int productId);
    
    /**
     * Sample usage:
     *
     * curl -X DELETE $HOST:$PORT/recommendation?productId=1
     *
     * @param productId
     */
    @DeleteMapping(value = "/recommendation")
    void deleteRecommendations(@RequestParam(value = "productId", required = true)  int productId);
}
