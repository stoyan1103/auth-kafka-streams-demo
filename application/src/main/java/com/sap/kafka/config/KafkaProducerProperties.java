package com.sap.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.producer")
public record KafkaProducerProperties(
    String clientId,
    String acks,
    Long lingerMs,
    Integer batchSize,
    Integer retries,
    String compressionType,
    Integer deliveryTimeoutMs,
    Integer requestTimeoutMs,
    Boolean enableIdempotence
) {
}
