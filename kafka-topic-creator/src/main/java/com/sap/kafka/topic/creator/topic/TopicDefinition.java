package com.sap.kafka.topic.creator.topic;

import java.util.Map;

public record TopicDefinition(String name, int partitions, short replicationFactor, Map<String, String> configs) {
}
