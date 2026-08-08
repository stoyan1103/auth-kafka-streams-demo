package com.sap.kafka.topic.creator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "topic-creator.kafka")
public record KafkaAdminProperties(String bootstrapServers, String clientId, Long operationTimeoutSeconds) {
}
