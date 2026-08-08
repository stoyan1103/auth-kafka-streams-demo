package com.sap.kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class KafkaPublisher<K, V> implements IKafkaPublisher<K, V>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisher.class);

    private final Producer<K, V> producer;

    public KafkaPublisher(Producer<K, V> producer) {
        this.producer = requireNonNull(producer, "Producer must not be null");
    }

    @Override
    public CompletableFuture<RecordMetadata> publish(String topic, K key, V value) {
        requireNonNull(topic, "Topic must not be null");
        requireNonNull(key, "Key must not be null");
        requireNonNull(value, "Value must not be null");

        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();

        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, value);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Failed to publish Kafka record. topic='{}', key='{}'", topic, key, exception);

                result.completeExceptionally(exception);
                return;
            }

            LOGGER.debug("Published Kafka record. topic='{}', key='{}', partition={}, offset={}", topic, key, metadata.partition(), metadata.offset());

            result.complete(metadata);
        });

        return result;
    }

    @Override
    public void close() {
        LOGGER.info("Closing Kafka publisher...");

        try {
            producer.close();
            LOGGER.info("Kafka publisher closed successfully.");
        } catch (Exception e) {
            LOGGER.error("Error while closing Kafka publisher.", e);
        }
    }
}
