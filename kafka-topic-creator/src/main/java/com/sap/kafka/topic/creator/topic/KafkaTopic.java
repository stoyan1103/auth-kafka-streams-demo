package com.sap.kafka.topic.creator.topic;

import java.util.HashMap;
import java.util.Map;

import com.sap.kafka.topic.creator.config.KafkaTopicDefaultsProperties;

import static com.sap.kafka.contract.KafkaTopics.NOTIFICATION_COMMANDS;
import static com.sap.kafka.contract.KafkaTopics.NOTIFICATION_STATUS;
import static com.sap.kafka.contract.KafkaTopics.TOKEN_SESSION_EVENTS;
import static com.sap.kafka.contract.KafkaTopics.USER_AUDIT_EVENTS;
import static com.sap.kafka.contract.KafkaTopics.USER_EVENTS;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_MS_CONFIG;

public enum KafkaTopic {

    USER_EVENTS_TOPIC(USER_EVENTS, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)),
    TOKEN_SESSION_EVENTS_TOPIC(TOKEN_SESSION_EVENTS, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)),
    NOTIFICATION_COMMANDS_TOPIC(NOTIFICATION_COMMANDS, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)),
    NOTIFICATION_STATUS_TOPIC(NOTIFICATION_STATUS, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE)),
    USER_AUDIT_EVENTS_TOPIC(USER_AUDIT_EVENTS, Map.of(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE));

    private final String topic;
    private final Map<String, String> topicConfigs;

    KafkaTopic(String topic, Map<String, String> topicConfigs) {
        this.topic = topic;
        this.topicConfigs = topicConfigs;
    }


    public TopicDefinition toDefinition(KafkaTopicDefaultsProperties defaults) {
        Map<String, String> configs = new HashMap<>();

        configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, defaults.minInsyncReplicas());
        configs.put(RETENTION_MS_CONFIG, defaults.retentionMs());
        configs.put(SEGMENT_MS_CONFIG, defaults.segmentMs());
        configs.put(COMPRESSION_TYPE_CONFIG, defaults.compressionType());

        configs.putAll(topicConfigs);

        return new TopicDefinition(
            topic,
            defaults.partitions(),
            defaults.replicationFactor(),
            Map.copyOf(configs)
        );
    }

    public String topic() {
        return topic;
    }
}
