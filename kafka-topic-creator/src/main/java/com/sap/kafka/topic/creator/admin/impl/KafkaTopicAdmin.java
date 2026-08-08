package com.sap.kafka.topic.creator.admin.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.kafka.topic.creator.admin.IKafkaTopicAdmin;
import com.sap.kafka.topic.creator.config.KafkaTopicDefaultsProperties;
import com.sap.kafka.topic.creator.topic.KafkaTopic;
import com.sap.kafka.topic.creator.topic.TopicDefinition;
import com.sap.lifecycle.ManagedLifecycle;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.clients.admin.NewPartitions.increaseTo;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

public class KafkaTopicAdmin implements IKafkaTopicAdmin, ManagedLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicAdmin.class);

    private final Properties adminProps;
    private final KafkaTopicDefaultsProperties topicDefaultsProperties;
    private final Long operationTimeout;

    public KafkaTopicAdmin(Properties adminProps, KafkaTopicDefaultsProperties topicDefaultsProperties, Long operationTimeout) {
        this.adminProps = requireNonNull(adminProps, "adminProps must not be null");
        this.topicDefaultsProperties = requireNonNull(topicDefaultsProperties, "topicDefaultsProperties must not be null");
        this.operationTimeout = requireNonNull(operationTimeout, "operationTimeout must not be null");
    }

    @Override
    public void init() {
        LOGGER.info("Starting Kafka topic provisioning...");

        List<TopicDefinition> topicDefinitions = Arrays.stream(KafkaTopic.values())
            .map(topic -> topic.toDefinition(topicDefaultsProperties))
            .toList();

        createOrUpdateTopics(topicDefinitions);

        LOGGER.info("Kafka topic provisioning finished successfully.");
    }

    @Override
    public void destroy() {
        LOGGER.info("Kafka topic provisioning task destroyed.");
    }

    @Override
    public void createOrUpdateTopics(Collection<TopicDefinition> topicDefinitions) {
        try (Admin admin = Admin.create(adminProps)) {
            Set<String> existingTopicNames = admin.listTopics().names().get(operationTimeout, SECONDS);

            Map<String, TopicDefinition> desiredDefinitions = topicDefinitions.stream()
                .collect(toMap(TopicDefinition::name, identity()));

            createMissingTopics(admin, desiredDefinitions, existingTopicNames);
            updateExistingTopics(admin, desiredDefinitions, existingTopicNames);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create or update Kafka topics", e);
        }
    }

    private void createMissingTopics(Admin admin, Map<String, TopicDefinition> desiredDefinitions, Set<String> existingTopicNames)
        throws Exception {
        List<NewTopic> topicsToCreate = desiredDefinitions.values()
            .stream()
            .filter(topic -> !existingTopicNames.contains(topic.name()))
            .map(KafkaTopicAdmin::toNewTopic)
            .toList();

        if (topicsToCreate.isEmpty()) {
            LOGGER.info("No missing Kafka topics to create.");
            return;
        }

        LOGGER.info("Creating Kafka topics: {}", topicsToCreate.stream().map(NewTopic::name).toList());

        admin.createTopics(topicsToCreate)
            .all()
            .get(operationTimeout, SECONDS);

        LOGGER.info("Kafka topics created successfully.");
    }

    private void updateExistingTopics(Admin admin, Map<String, TopicDefinition> desiredDefinitions, Set<String> existingTopicNames)
        throws Exception {

        Map<String, TopicDefinition> desired = desiredDefinitions.entrySet()
            .stream()
            .filter(entry -> existingTopicNames.contains(entry.getKey()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (desired.isEmpty()) {
            return;
        }

        Map<String, TopicDescription> currentDescriptions = admin
            .describeTopics(desired.keySet())
            .allTopicNames()
            .get(operationTimeout, SECONDS);

        increasePartitionsIfNeeded(admin, desired, currentDescriptions);
        warnIfReplicationFactorDiffers(desired, currentDescriptions);
        updateTopicConfigs(admin, desired);
    }

    private void increasePartitionsIfNeeded(Admin admin, Map<String, TopicDefinition> desiredDefinitions, Map<String, TopicDescription> actualMap)
        throws Exception {

        Map<String, NewPartitions> partitionsToIncrease = new HashMap<>();

        for (Map.Entry<String, TopicDefinition> entry : desiredDefinitions.entrySet()) {
            String topicName = entry.getKey();
            TopicDefinition desired = entry.getValue();

            int currentPartitions = actualMap.get(topicName).partitions().size();
            int desiredPartitions = desired.partitions();

            if (desiredPartitions > currentPartitions) {
                partitionsToIncrease.put(topicName, increaseTo(desiredPartitions));
                LOGGER.info("Topic '{}' partitions will be increased from {} to {}.", topicName, currentPartitions, desiredPartitions);
            } else if (desiredPartitions < currentPartitions) {
                LOGGER.warn("Topic '{}' has {} partitions, more than desired {}. Kafka cannot decrease partitions.",
                    topicName, currentPartitions, desiredPartitions);
            }
        }

        if (partitionsToIncrease.isEmpty()) {
            LOGGER.info("No Kafka topics require partition increases.");
            return;
        }

        admin.createPartitions(partitionsToIncrease)
            .all()
            .get(operationTimeout, SECONDS);
    }

    private void warnIfReplicationFactorDiffers(Map<String, TopicDefinition> desiredDefinitions, Map<String, TopicDescription> actualMap) {
        for (Map.Entry<String, TopicDescription> entry : actualMap.entrySet()) {
            String topicName = entry.getKey();
            TopicDescription actualDesc = entry.getValue();

            int desiredRF = desiredDefinitions.get(topicName).replicationFactor();

            boolean mismatch = actualDesc.partitions().stream()
                .anyMatch(partition -> partition.replicas().size() != desiredRF);

            if (mismatch) {
                LOGGER.warn("Topic '{}' replication factor mismatch. Desired: {}. Note: AdminClient cannot auto-fix RF.",
                    topicName, desiredRF);
            }
        }
    }

    private void updateTopicConfigs(Admin admin, Map<String, TopicDefinition> desiredDefinitions) throws ExecutionException, InterruptedException, TimeoutException {
        Map<ConfigResource, Collection<AlterConfigOp>> configChanges = new HashMap<>();
        for (TopicDefinition desiredDefinition : desiredDefinitions.values()) {
            ConfigResource resource = topicResource(desiredDefinition.name());

            Config currentConfig = admin
                .describeConfigs(Set.of(resource))
                .all()
                .get(operationTimeout, SECONDS)
                .get(resource);

            List<AlterConfigOp> operations = desiredDefinition.configs()
                .entrySet()
                .stream()
                .filter(desiredConfig -> configNeedsUpdate(currentConfig, desiredConfig))
                .map(desiredConfig -> new AlterConfigOp(
                        new ConfigEntry(
                            desiredConfig.getKey(),
                            desiredConfig.getValue()
                        ),
                        SET
                    )
                )
                .toList();

            if (!operations.isEmpty()) {
                configChanges.put(resource, operations);
            }
        }

        if (configChanges.isEmpty()) {
            LOGGER.info("No topic configs need to be updated.");
            return;
        }

        List<String> affectedTopics = configChanges.keySet().stream()
            .map(ConfigResource::name)
            .sorted()
            .toList();

        LOGGER.info("Updating Kafka topic configs for topics: {}", affectedTopics);

        admin.incrementalAlterConfigs(configChanges)
            .all()
            .get(operationTimeout, SECONDS);

        LOGGER.info("Kafka topic configs updated successfully for topics: {}", affectedTopics);
    }

    private static NewTopic toNewTopic(TopicDefinition definition) {
        return new NewTopic(definition.name(), definition.partitions(), definition.replicationFactor())
            .configs(definition.configs());
    }

    private static ConfigResource topicResource(String name) {
        return new ConfigResource(TOPIC, name);
    }

    private static boolean configNeedsUpdate(Config currentConfig, Map.Entry<String, String> desiredConfig) {
        ConfigEntry currentEntry = currentConfig.get(desiredConfig.getKey());

        return currentEntry == null || !desiredConfig.getValue().equals(currentEntry.value());
    }
}
