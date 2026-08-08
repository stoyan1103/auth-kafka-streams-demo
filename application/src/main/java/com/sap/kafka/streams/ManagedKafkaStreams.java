package com.sap.kafka.streams;

import java.time.Duration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

public class ManagedKafkaStreams {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedKafkaStreams.class);

    private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaStreams streams;
    private final String applicationId;
    private final Duration stopTimeoutDuration;

    public ManagedKafkaStreams(KafkaStreams streams, String applicationId, Duration stopTimeoutDuration) {
        this.streams = requireNonNull(streams, "streams must not be null");
        this.applicationId = requireNonNull(applicationId, "applicationId must not be null");
        this.stopTimeoutDuration = requireNonNullElse(stopTimeoutDuration, DEFAULT_STOP_TIMEOUT);

        this.streams.setStateListener(this::onStateChange);
        this.streams.setUncaughtExceptionHandler(this::onUncaughtException);
        this.streams.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
    }

    public void start() {
        LOGGER.info("[{}] Starting Kafka Streams application...", applicationId);
        streams.start();
    }

    public void stop() {
        LOGGER.info("[{}] Stopping Kafka Streams application...", applicationId);
        streams.close(stopTimeoutDuration);
        LOGGER.info("[{}] Kafka Streams application stopped.", applicationId);
    }

    public State getState() {
        return streams.state();
    }

    public <T> T getStore(StoreQueryParameters<T> storeQueryParameters) {
        return streams.store(storeQueryParameters);
    }

    private void onStateChange(State newState, State oldState) {
        LOGGER.info("[{}] Kafka Streams state transition: {} -> {}", applicationId, oldState, newState);
    }

    private StreamThreadExceptionResponse onUncaughtException(Throwable e) {
        LOGGER.error("Uncaught exception in Kafka Streams thread - replacing thread", e);
        return REPLACE_THREAD;
    }

    private final class LoggingStateRestoreListener implements StateRestoreListener {

        @Override
        public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
            LOGGER.info("[{}] State restore START: store=[{}] partition=[{}-{}] offsets {}..{} ({} records to restore)", applicationId, storeName, topicPartition.topic(), topicPartition.partition(), startingOffset, endingOffset, endingOffset - startingOffset);
        }

        @Override
        public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
            // Fires very frequently during replay -> DEBUG to avoid flooding
            LOGGER.debug("[{}] State restore BATCH: store=[{}] partition=[{}-{}] batchEndOffset={} restored={}", applicationId, storeName, topicPartition.topic(), topicPartition.partition(), batchEndOffset, numRestored);
        }

        @Override
        public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
            LOGGER.info("[{}] State restore END: store=[{}] partition=[{}-{}] totalRestored={}", applicationId, storeName, topicPartition.topic(), topicPartition.partition(), totalRestored);
        }
    }
}
