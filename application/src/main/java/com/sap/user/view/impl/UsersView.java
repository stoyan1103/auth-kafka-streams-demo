package com.sap.user.view.impl;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import com.sap.authkafkastreams.proto.user.UserEvent;
import com.sap.authkafkastreams.proto.user.UserView;
import com.sap.kafka.serializer.ProtobufSerde;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.kafka.streams.KafkaStreamsSpec;
import com.sap.kafka.streams.ManagedKafkaStreams;
import com.sap.lifecycle.ManagedLifecycle;
import com.sap.user.view.IUsersView;

import static com.sap.kafka.contract.KafkaTopics.USER_EVENTS;
import static com.sap.kafka.utils.KafkaUtils.getOne;
import static com.sap.kafka.utils.KafkaUtils.getStore;
import static java.util.Objects.requireNonNull;

public class UsersView implements IUsersView, ManagedLifecycle {

    public static final String AUTH_USER_STREAMS_APP_ID = "auth-user-streams-v1";
    private static final String SOURCE_NODE = "user-events-source";
    private static final String PROCESSOR_NODE = "users-view-materializer";
    private static final String USERS_VIEW_STORE = "users-view-store";

    private final KafkaStreamsFactory factory;
    private ManagedKafkaStreams streams;

    public UsersView(KafkaStreamsFactory factory) {
        this.factory = requireNonNull(factory, "factory must not be null");
    }

    @Override
    public void init() {
        this.streams = factory.createStreams(KafkaStreamsSpec.of(AUTH_USER_STREAMS_APP_ID), buildTopology());
        this.streams.start();
    }

    @Override
    public void destroy() {
        if (streams != null) {
            streams.stop();
        }
    }

    @Override
    public Optional<UserView> findById(String userId) {
        requireNonNull(userId, "userId must not be null");
        return getOne(getStore(streams, USERS_VIEW_STORE), userId);
    }

    private static Topology buildTopology() {
        Topology topology = new Topology();

        topology.addSource(
            SOURCE_NODE,
            Serdes.String().deserializer(),
            ProtobufSerde.of(UserEvent.parser()).deserializer(),
            USER_EVENTS
        );

        topology.addProcessor(
            PROCESSOR_NODE,
            () -> new UsersViewMaterializer(USERS_VIEW_STORE),
            SOURCE_NODE
        );

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(USERS_VIEW_STORE),
                Serdes.String(),
                ProtobufSerde.of(UserView.parser())
            ),
            PROCESSOR_NODE
        );

        return topology;
    }
}
