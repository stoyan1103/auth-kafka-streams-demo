package com.sap.kafka.topic.creator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "topic-creator.kafka.topic-defaults")
public record KafkaTopicDefaultsProperties(
    int partitions,
    short replicationFactor,
    String minInsyncReplicas,
    String retentionMs,
    String segmentMs,
    String compressionType) {
}
