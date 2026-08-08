package com.sap.kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface IKafkaPublisher<K, V> {

    CompletableFuture<RecordMetadata> publish(String topic, K key, V value);
}
