package com.sap.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.streams")
public record KafkaStreamsProperties(String stateDir) {
}
