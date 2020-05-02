package com.frank.microservices.composite.product;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.stream.test.matcher.MessageQueueMatcher;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.frank.api.core.api.composite.product.ProductAggregate;
import com.frank.api.core.api.composite.product.RecommendationSummary;
import com.frank.api.core.api.composite.product.ReviewSummary;
import com.frank.api.core.api.core.product.Product;
import com.frank.api.core.api.core.recommendation.Recommendation;
import com.frank.api.core.api.core.review.Review;
import com.frank.api.core.api.event.Event;
import com.frank.microservices.composite.product.services.ProductCompositeIntegration;

import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment=WebEnvironment.RANDOM_PORT)
public class MessagingTest {
	
//	private static final int PRODUCT_ID_OK = 1;
//	private static final int PRODUCT_ID_NOT_FOUND = 2;
//	private static final int PRODUCT_ID_INVALID = 3;
	
	@Autowired
	private WebTestClient client;
	
	@Autowired
	private ProductCompositeIntegration.MessageSources channels;
	
	@Autowired
	private MessageCollector collector;

	BlockingQueue<Message<?>> queueProducts = null;
	BlockingQueue<Message<?>> queueRecommendations = null;
	BlockingQueue<Message<?>> queueReviews = null;
	
	@BeforeEach
	public void setUp() {
		queueProducts = getQueue(channels.outputProducts());
		queueRecommendations = getQueue(channels.outputRecommendations());
		queueReviews = getQueue(channels.outputReviews());
	}
	
	private BlockingQueue<Message<?>> getQueue(MessageChannel messageChannel) {
		return collector.forChannel(messageChannel);
	}
	
	@Test
	public void createCompositeProduct1() {

		ProductAggregate composite = new ProductAggregate(1, "name", 1, null, null, null);
		postAndVerifyProduct(composite, HttpStatus.OK);

		// Assert one expected new product events queued up
		assertEquals(1, queueProducts.size());

		Event<Integer, Product> expectedEvent = new Event<Integer, Product>(
				Event.Type.CREATE, 
				composite.getProductId(), 
				new Product(composite.getProductId(), composite.getName(), composite.getWeight(), null));
		MatcherAssert.assertThat(queueProducts, 
				Matchers
					.is(MessageQueueMatcher
							.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedEvent))));

		// Assert none recommendations and review events
		assertEquals(0, queueRecommendations.size());
		assertEquals(0, queueReviews.size());
	}
	
	@Test
	public void createCompositeProduct2() {

		ProductAggregate composite = new ProductAggregate(1, "name", 1,
			Collections.singletonList(new RecommendationSummary(1, "a", 1, "c")),
			Collections.singletonList(new ReviewSummary(1, "a", "s", "c")), null);

		postAndVerifyProduct(composite, HttpStatus.OK);

		// Assert one create product event queued up
		assertEquals(1, queueProducts.size());

		Event<Integer, Product> expectedProductEvent = new Event<>(Event.Type.CREATE, 
				composite.getProductId(), new Product(composite.getProductId(), composite.getName(),
						composite.getWeight(), null));
		MatcherAssert.assertThat(queueProducts, 
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedProductEvent)));

		// Assert one create recommendation event queued up
		assertEquals(1, queueRecommendations.size());

		RecommendationSummary rec = composite.getRecommendations().get(0);
		Event<Integer, Recommendation> expectedRecommendationEvent = new Event<>(Event.Type.CREATE, 
				composite.getProductId(), new Recommendation(composite.getProductId(), 
						rec.getRecommendationId(), rec.getAuthor(), rec.getRate(), 
						rec.getContent(), null));
		MatcherAssert.assertThat(queueRecommendations, 
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedRecommendationEvent)));

		// Assert one create review event queued up
		assertEquals(1, queueReviews.size());

		ReviewSummary rev = composite.getReviews().get(0);
		Event<Integer, Review> expectedReviewEvent = new Event<>(Event.Type.CREATE, 
				composite.getProductId(), new Review(composite.getProductId(), rev.getReviewId(), 
						rev.getAuthor(), rev.getSubject(), rev.getContent(), null));
		MatcherAssert.assertThat(queueReviews, 
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedReviewEvent)));
	}

	@Test
	public void deleteCompositeProduct() {

		deleteAndVerifyProduct(1, HttpStatus.OK);

		// Assert one delete product event queued up
		assertEquals(1, queueProducts.size());

		Event<Integer, Product> expectedEvent = new Event<>(Event.Type.DELETE, 1, null);
		MatcherAssert.assertThat(queueProducts, Matchers.is(
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedEvent))));

		// Assert one delete recommendation event queued up
		assertEquals(1, queueRecommendations.size());

		Event<Integer, Product> expectedRecommendationEvent = new Event<>(Event.Type.DELETE, 1, null);
		MatcherAssert.assertThat(queueRecommendations, 
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedRecommendationEvent)));

		// Assert one delete review event queued up
		assertEquals(1, queueReviews.size());

		Event<Integer, Product> expectedReviewEvent = new Event<>(Event.Type.DELETE, 1, null);
		MatcherAssert.assertThat(queueReviews, 
				MessageQueueMatcher
					.receivesPayloadThat(IsSameEvent.sameEventExceptCreatedAt(expectedReviewEvent)));
	}
	
	private void postAndVerifyProduct(ProductAggregate compositeProduct, HttpStatus expectedStatus) {
		client.post()
			.uri("/product-composite")
			.body(Mono.just(compositeProduct), ProductAggregate.class)
			.exchange()
			.expectStatus().isEqualTo(expectedStatus);
	}
	
	private void deleteAndVerifyProduct(int productId, HttpStatus expectedStatus) {
		client.delete()
			.uri("/product-composite/" + productId)
			.exchange()
			.expectStatus().isEqualTo(expectedStatus);
	}
}
