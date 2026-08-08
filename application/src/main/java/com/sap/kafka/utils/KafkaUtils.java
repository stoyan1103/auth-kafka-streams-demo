package com.sap.kafka.utils;

import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.sap.exception.StoreUnavailableException;
import com.sap.kafka.streams.ManagedKafkaStreams;

import static com.sap.thread.ThreadUtils.sleepSafely;
import static java.lang.String.format;
import static org.apache.kafka.streams.KafkaStreams.State.CREATED;
import static org.apache.kafka.streams.KafkaStreams.State.REBALANCING;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

public final class KafkaUtils {

    private static final Duration STORE_READY_TIMEOUT = Duration.ofSeconds(15);
    private static final long POLL_INTERVAL_MS = 100L;

    private KafkaUtils() {
    }

    public static <K, V> ReadOnlyKeyValueStore<K, V> getStore(ManagedKafkaStreams streams, String storeName) {
        long deadline = System.nanoTime() + STORE_READY_TIMEOUT.toNanos();
        waitForRunningStream(streams, deadline);
        return getStoreWithRetry(streams, storeName, keyValueStore(), deadline);
    }

    public static <V> Optional<V> getOne(ReadOnlyKeyValueStore<String, V> store, String key) {
        return Optional.ofNullable(store.get(key));
    }

    private static void waitForRunningStream(ManagedKafkaStreams streams, long deadline) {
        while (streams.getState() != RUNNING) {
            if (Thread.interrupted()) {
                throw new IllegalStateException(format("Thread interrupted while waiting for stream to reach RUNNING, state: %s", streams.getState()));
            }

            boolean isBeforeDeadline = System.nanoTime() < deadline;
            boolean isTransientState = streams.getState() == CREATED || streams.getState() == REBALANCING;

            if (isBeforeDeadline && isTransientState) {
                sleepSafely(POLL_INTERVAL_MS);
                continue;
            }

            throw new StoreUnavailableException(format("Stream did not reach RUNNING within %ds (state=%s)", STORE_READY_TIMEOUT.toSeconds(), streams.getState()));
        }
    }

    private static <T> T getStoreWithRetry(ManagedKafkaStreams streams, String storeName, QueryableStoreType<T> storeType, long deadline) {
        while (System.nanoTime() < deadline) {
            try {
                return streams.getStore(fromNameAndType(storeName, storeType));
            } catch (InvalidStateStoreException _) {
                sleepSafely(POLL_INTERVAL_MS);
            }
        }

        throw new StoreUnavailableException(format("State store '%s' not queryable after %d seconds", storeName, STORE_READY_TIMEOUT.toSeconds()));
    }
}
