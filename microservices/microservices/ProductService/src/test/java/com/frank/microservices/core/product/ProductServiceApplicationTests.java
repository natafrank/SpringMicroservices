package com.frank.microservices.core.product;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.MediaType.APPLICATION_JSON;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.http.HttpStatus;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.frank.api.core.api.core.product.Product;
import com.frank.api.core.api.event.Event;
import com.frank.microservices.core.product.persistence.ProductRepository;
import com.frank.util.exceptions.InvalidInputException;

@SpringBootTest(webEnvironment=RANDOM_PORT,
		properties = {
			"spring.data.mongodb.port: 0",
			"eureka.client.enabled=false"
		})
class ProductServiceApplicationTests {

    @Autowired
    private WebTestClient client;
    
    @Autowired
    ProductRepository repository;
    
    @Autowired
	private Sink channels;
    
    private AbstractMessageChannel input = null;
    
    @BeforeEach
    public void setUpDb() {
    	input = (AbstractMessageChannel) channels.input();
    	repository.deleteAll().block();
    }
    
    @Test
	public void getProductById() {
    	int productId = 1;

		assertNull(repository.findByProductId(productId).block());
		assertEquals(0, (long)repository.count().block());

		sendCreateProductEvent(productId);

		assertNotNull(repository.findByProductId(productId).block());
		assertEquals(1, (long)repository.count().block());

		getAndVerifyProduct(productId, HttpStatus.OK)
            .jsonPath("$.productId").isEqualTo(productId);
	}
    
    @Test
    public void duplicateError() {
    	int productId = 1;

		assertNull(repository.findByProductId(productId).block());

		sendCreateProductEvent(productId);

		assertNotNull(repository.findByProductId(productId).block());

		try {
			sendCreateProductEvent(productId);
			fail("Expected a MessagingException here!");
		} catch (MessagingException me) {
			if (me.getCause() instanceof InvalidInputException)	{
				InvalidInputException iie = (InvalidInputException)me.getCause();
				assertEquals("Duplicate key, Product Id: " + productId, iie.getMessage());
			} else {
				fail("Expected a InvalidInputException as the root cause!");
			}
		}
    }
    
    @Test
	public void deleteProduct() {
    	int productId = 1;

		sendCreateProductEvent(productId);
		assertNotNull(repository.findByProductId(productId).block());

		sendDeleteProductEvent(productId);
		assertNull(repository.findByProductId(productId).block());

		sendDeleteProductEvent(productId);
	}
    
    @Test
	public void getProductInvalidParameterString() {
    	getAndVerifyProduct("/no-integer", HttpStatus.BAD_REQUEST)
        .jsonPath("$.path").isEqualTo("/product/no-integer")
        .jsonPath("$.message").isEqualTo("Type mismatch.");
	}
    
    @Test
	public void getProductNotFound() {
		int productIdNotFound = 13;
		getAndVerifyProduct(productIdNotFound, HttpStatus.NOT_FOUND)
            .jsonPath("$.path").isEqualTo("/product/" + productIdNotFound)
            .jsonPath("$.message").isEqualTo("No product found for productId: " + productIdNotFound);
	}

	@Test
	public void getProductInvalidParameterNegativeValue() {

        int productIdInvalid = -1;

		getAndVerifyProduct(productIdInvalid, HttpStatus.UNPROCESSABLE_ENTITY)
            .jsonPath("$.path").isEqualTo("/product/" + productIdInvalid)
            .jsonPath("$.message").isEqualTo("Invalid productId: " + productIdInvalid);
	}
    
    private WebTestClient.BodyContentSpec getAndVerifyProduct(int productId, HttpStatus expectedStatus) {
		return getAndVerifyProduct("/" + productId, expectedStatus);
	}
    
    private WebTestClient.BodyContentSpec getAndVerifyProduct(String productIdPath, HttpStatus expectedStatus) {
		return client.get()
			.uri("/product" + productIdPath)
			.accept(APPLICATION_JSON)
			.exchange()
			.expectStatus().isEqualTo(expectedStatus)
			.expectHeader().contentType(APPLICATION_JSON)
			.expectBody();
	}
    
	private void sendCreateProductEvent(int productId) {
		Product product = new Product(productId, "Name " + productId, productId, "SA");
		Event<Integer, Product> event = new Event<>(Event.Type.CREATE, productId, product);
		input.send(new GenericMessage<>(event));
	}

	private void sendDeleteProductEvent(int productId) {
		Event<Integer, Product> event = new Event<>(Event.Type.DELETE, productId, null);
		input.send(new GenericMessage<>(event));
	}
}
