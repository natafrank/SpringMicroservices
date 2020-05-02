package com.frank.springcloud.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.CompositeReactiveHealthContributor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthContributorRegistry;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class HealthCheckConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckConfiguration.class);

//    @Autowired
//    private StatusAggregator statusAggregator;

    private final WebClient.Builder webClientBuilder;

    private WebClient webClient;

    @Autowired
    private ReactiveHealthContributorRegistry registry;

    @Autowired
    public HealthCheckConfiguration(WebClient.Builder webClientBuilder) {
        this.webClientBuilder = webClientBuilder;
    }

    @Bean
    CompositeReactiveHealthContributor coreServices() {
        ReactiveHealthIndicator productHealth           = () -> getHealth("http://product");
        ReactiveHealthIndicator recommendationHealth    = () -> getHealth("http://recommendation");
        ReactiveHealthIndicator reviewHealth            = () -> getHealth("http://review");
        ReactiveHealthIndicator productCompositeHealth  = () -> getHealth("http://product-composite");
        Map<String, ReactiveHealthIndicator> map = new HashMap<>();

        map.put("products", productHealth);
        map.put("recommendation", recommendationHealth);
        map.put("review", reviewHealth);
        map.put("product-composite", productCompositeHealth);

        return CompositeReactiveHealthContributor.fromMap(map);
    }

    private Mono<Health> getHealth(String url) {
        url += "/actuator/health";
        LOG.debug("Will call the Health API on URL: {}", url);
        return getWebClient().get().uri(url).retrieve().bodyToMono(String.class)
                .map(s -> new Health.Builder().up().build())
                .onErrorResume(ex -> Mono.just(new Health.Builder().down(ex).build()))
                .log();
    }

    private WebClient getWebClient() {
        if (webClient == null) {
            webClient = webClientBuilder.build();
        }
        return webClient;
    }
}
