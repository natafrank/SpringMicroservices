package com.frank.microservices.core.recommendation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.frank.api.core.api.core.recommendation.Recommendation;
import com.frank.api.core.api.event.Event;
import com.frank.microservices.core.recommendation.persistence.RecommendationRepository;
import com.frank.util.exceptions.InvalidInputException;

@SpringBootTest(webEnvironment=RANDOM_PORT,
		properties = {
			"spring.data.mongodb.port: 0",
			"eureka.client.enabled=false"
		})
class RecommendationServiceApplicationTests {

	@Autowired
	private WebTestClient client;
	
	@Autowired
	private RecommendationRepository repository;

	@Autowired
	private Sink channels;
	
	private AbstractMessageChannel input = null;

	@BeforeEach
	public void setupDb() {
		input = (AbstractMessageChannel) channels.input();
		repository.deleteAll().block();
	}
	
	@Test
	public void getRecommendationsByProductId() {

		int productId = 1;

		sendCreateRecommendationEvent(productId, 1);
		sendCreateRecommendationEvent(productId, 2);
		sendCreateRecommendationEvent(productId, 3);

		assertEquals(3, (long)repository.findByProductId(productId).count().block());

		getAndVerifyRecommendationsByProductId(productId, HttpStatus.OK)
			.jsonPath("$.length()").isEqualTo(3)
			.jsonPath("$[2].productId").isEqualTo(productId)
			.jsonPath("$[2].recommendationId").isEqualTo(3);
	}

	@Test
	public void duplicateError() {

		int productId = 1;
		int recommendationId = 1;

		sendCreateRecommendationEvent(productId, recommendationId);
		assertEquals(1, (long)repository.count().block());

		try {
			sendCreateRecommendationEvent(productId, recommendationId);
			fail("Expected a MessagingException here!");
		} catch (MessagingException me) {
			if (me.getCause() instanceof InvalidInputException)	{
				InvalidInputException iie = (InvalidInputException)me.getCause();
				assertEquals("Duplicate key, Product Id: 1, Recommendation Id:1", iie.getMessage());
			} else {
				fail("Expected a InvalidInputException as the root cause!");
			}
		}

		assertEquals(1, (long)repository.count().block());
	}

	@Test
	public void deleteRecommendations() {

		int productId = 1;
		int recommendationId = 1;

		sendCreateRecommendationEvent(productId, recommendationId);
		assertEquals(1, (long)repository.findByProductId(productId).count().block());

		sendDeleteRecommendationEvent(productId);
		assertEquals(0, (long)repository.findByProductId(productId).count().block());

		sendDeleteRecommendationEvent(productId);
	}

	@Test
	public void getRecommendationsMissingParameter() {

		getAndVerifyRecommendationsByProductId("", HttpStatus.BAD_REQUEST)
			.jsonPath("$.path").isEqualTo("/recommendation")
			.jsonPath("$.message").isEqualTo("Required int parameter 'productId' is not present");
	}

	@Test
	public void getRecommendationsInvalidParameter() {

		getAndVerifyRecommendationsByProductId("?productId=no-integer", HttpStatus.BAD_REQUEST)
			.jsonPath("$.path").isEqualTo("/recommendation")
			.jsonPath("$.message").isEqualTo("Type mismatch.");
	}

	@Test
	public void getRecommendationsNotFound() {

		getAndVerifyRecommendationsByProductId("?productId=113", HttpStatus.OK)
			.jsonPath("$.length()").isEqualTo(0);
	}

	@Test
	public void getRecommendationsInvalidParameterNegativeValue() {

		int productIdInvalid = -1;

		getAndVerifyRecommendationsByProductId("?productId=" + productIdInvalid, HttpStatus.UNPROCESSABLE_ENTITY)
			.jsonPath("$.path").isEqualTo("/recommendation")
			.jsonPath("$.message").isEqualTo("Invalid productId: " + productIdInvalid);
	}

	private WebTestClient.BodyContentSpec getAndVerifyRecommendationsByProductId(int productId, HttpStatus expectedStatus) {
		return getAndVerifyRecommendationsByProductId("?productId=" + productId, expectedStatus);
	}

	private WebTestClient.BodyContentSpec getAndVerifyRecommendationsByProductId(String productIdQuery, 
			HttpStatus expectedStatus) {
		return client.get()
			.uri("/recommendation" + productIdQuery)
			.accept(MediaType.APPLICATION_JSON)
			.exchange()
			.expectStatus().isEqualTo(expectedStatus)
			.expectHeader().contentType(MediaType.APPLICATION_JSON)
			.expectBody();
	}
	
	private void sendCreateRecommendationEvent(int productId, int recommendationId) {
		Recommendation recommendation = new Recommendation(productId, recommendationId, 
				"Author " + recommendationId, recommendationId, "Content " + recommendationId, "SA");
		Event<Integer, Recommendation> event = new Event<>(Event.Type.CREATE, productId, recommendation);
		input.send(new GenericMessage<>(event));
	}

	private void sendDeleteRecommendationEvent(int productId) {
		Event<Integer, Recommendation> event = new Event<>(Event.Type.DELETE, productId, null);
		input.send(new GenericMessage<>(event));
	}
}
