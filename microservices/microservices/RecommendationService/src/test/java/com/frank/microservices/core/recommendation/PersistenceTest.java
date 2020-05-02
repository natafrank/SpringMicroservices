package com.frank.microservices.core.recommendation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;

import com.frank.microservices.core.recommendation.persistence.RecommendationEntity;
import com.frank.microservices.core.recommendation.persistence.RecommendationRepository;

@DataMongoTest
public class PersistenceTest {

    @Autowired
    private RecommendationRepository repository;

    private RecommendationEntity savedEntity;

    @BeforeEach
   	public void setupDb() {
    	repository.deleteAll().block();

        RecommendationEntity entity = new RecommendationEntity(1, 2, "a", 3, "c");
        savedEntity = repository.save(entity).block();

        assertEqualsRecommendation(entity, savedEntity); 	
    }

    @Test
   	public void create() {
    	RecommendationEntity newEntity = new RecommendationEntity(1, 3, "a", 3, "c");
        repository.save(newEntity).block();

        RecommendationEntity foundEntity = repository.findById(newEntity.getId()).block();
        assertEqualsRecommendation(newEntity, foundEntity);

        assertEquals(2, (long)repository.count().block());
    }

    @Test
   	public void update() {
    	savedEntity.setAuthor("a2");
    	repository.save(savedEntity).block();
    	
    	RecommendationEntity foundEntity = repository.findById(savedEntity.getId()).block();
    	assertEquals(1, (long)foundEntity.getVersion());
    	assertEquals("a2", foundEntity.getAuthor());
    }

    @Test
   	public void delete() {
    	repository.delete(savedEntity).block();
    	assertFalse(repository.existsById(savedEntity.getId()).block());
    }

    @Test
   	public void getByProductId() {
    	List<RecommendationEntity> entityList = repository
    			.findByProductId(savedEntity.getProductId()).collectList().block();
    	assertThat(entityList, Matchers.hasSize(1));
    }

    @Test
   	public void duplicateError() {
    	assertThrows(DuplicateKeyException.class,
    			() -> {
    		    		RecommendationEntity entity = new RecommendationEntity(1, 2, "a", 3, "c");
    		        	savedEntity = repository.save(entity).block();
    			});
    }

    @Test
   	public void optimisticLockError() {
    	// Store the saved entity in two separate entity objects
        RecommendationEntity entity1 = repository.findById(savedEntity.getId()).block();
        RecommendationEntity entity2 = repository.findById(savedEntity.getId()).block();
        
        entity1.setAuthor("a1");
        repository.save(entity1).block();
        
        assertThrows(OptimisticLockingFailureException.class, 
        		() -> {
        			entity2.setAuthor("a2");
        			repository.save(entity2).block();
        		});
        
        RecommendationEntity updatedEntity = repository.findById(savedEntity.getId()).block();
        assertEquals(1,  (int)updatedEntity.getVersion());
        assertEquals("a1", updatedEntity.getAuthor());
    }

    private void assertEqualsRecommendation(RecommendationEntity expectedEntity, RecommendationEntity actualEntity) {
    	assertEquals(expectedEntity.getId(),               actualEntity.getId());
        assertEquals(expectedEntity.getVersion(),          actualEntity.getVersion());
        assertEquals(expectedEntity.getProductId(),        actualEntity.getProductId());
        assertEquals(expectedEntity.getRecommendationId(), actualEntity.getRecommendationId());
        assertEquals(expectedEntity.getAuthor(),           actualEntity.getAuthor());
        assertEquals(expectedEntity.getRating(),           actualEntity.getRating());
        assertEquals(expectedEntity.getContent(),          actualEntity.getContent());
    }
}
