package com.sap.kafka.topic.creator.admin;

import java.util.Collection;

import com.sap.kafka.topic.creator.topic.TopicDefinition;

public interface IKafkaTopicAdmin {

    void createOrUpdateTopics(Collection<TopicDefinition> topicDefinitions);
}
