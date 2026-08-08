package com.sap.kafka.streams;

import java.util.Properties;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.OPTIMIZE;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;

public class KafkaStreamsFactory {

    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_NUM_STREAM_THREADS = 1;
    private static final long DEFAULT_COMMIT_INTERVAL_MS = 1000L;
    private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

    private final String bootstrapServers;
    private final String stateDir;

    public KafkaStreamsFactory(String bootstrapServers, String stateDir) {
        this.bootstrapServers = requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        this.stateDir = requireNonNull(stateDir, "stateDir must not be null");
    }

    private Properties buildProps(KafkaStreamsSpec spec) {
        Properties props = new Properties();

        props.put(APPLICATION_ID_CONFIG, spec.applicationId());
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(STATE_DIR_CONFIG, format("%s/%s", stateDir, spec.applicationId()));
        props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(REPLICATION_FACTOR_CONFIG, DEFAULT_REPLICATION_FACTOR);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, OPTIMIZE);
        props.put(NUM_STREAM_THREADS_CONFIG, requireNonNullElse(spec.numStreamThreads(), DEFAULT_NUM_STREAM_THREADS));
        props.put(COMMIT_INTERVAL_MS_CONFIG, requireNonNullElse(spec.commitIntervalMs(), DEFAULT_COMMIT_INTERVAL_MS));
        props.put(consumerPrefix(AUTO_OFFSET_RESET_CONFIG), requireNonNullElse(spec.autoOffsetReset(), DEFAULT_AUTO_OFFSET_RESET));
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return props;
    }

    public ManagedKafkaStreams createStreams(KafkaStreamsSpec spec, Topology topology) {
        requireNonNull(spec, "spec must not be null");
        requireNonNull(topology, "topology must not be null");

        KafkaStreams streams = new KafkaStreams(topology, buildProps(spec));
        return new ManagedKafkaStreams(streams, spec.applicationId(), spec.stopTimeoutDuration());
    }
}
