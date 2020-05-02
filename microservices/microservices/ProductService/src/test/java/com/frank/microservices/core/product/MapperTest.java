package com.frank.microservices.core.product;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import com.frank.api.core.api.core.product.Product;
import com.frank.microservices.core.product.persistence.ProductEntity;
import com.frank.microservices.core.product.services.ProductMapper;

public class MapperTest {
	private ProductMapper mapper = Mappers.getMapper(ProductMapper.class);
	
	@Test
    public void mapperTests() {

        Assertions.assertNotNull(mapper);

        Product api = new Product(1, "n", 1, "sa");

        ProductEntity entity = mapper.apiToEntity(api);

        Assertions.assertEquals(api.getProductId(), entity.getProductId());
        Assertions.assertEquals(api.getProductId(), entity.getProductId());
        Assertions.assertEquals(api.getName(), entity.getName());
        Assertions.assertEquals(api.getWeight(), entity.getWeight());

        Product api2 = mapper.entityToApi(entity);

        Assertions.assertEquals(api.getProductId(), api2.getProductId());
        Assertions.assertEquals(api.getProductId(), api2.getProductId());
        Assertions.assertEquals(api.getName(),      api2.getName());
        Assertions.assertEquals(api.getWeight(),    api2.getWeight());
        Assertions.assertNull(api2.getServiceAddress());
    }
}
