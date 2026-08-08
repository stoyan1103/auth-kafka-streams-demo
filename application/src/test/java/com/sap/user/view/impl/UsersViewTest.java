package com.sap.user.view.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.sap.authkafkastreams.proto.user.UserEvent;
import com.sap.authkafkastreams.proto.user.UserRegistered;
import com.sap.authkafkastreams.proto.user.UserView;
import com.sap.kafka.serializer.ProtobufSerde;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.kafka.streams.KafkaStreamsSpec;
import com.sap.kafka.streams.ManagedKafkaStreams;

import static com.sap.kafka.contract.KafkaTopics.USER_EVENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UsersViewTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, UserEvent> inputTopic;
    private KeyValueStore<String, UserView> usersStore;

    @BeforeEach
    void setUp() {
        setupTopology();

        assertThat(usersStore).isNotNull();
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void givenUserRegisteredEvent_whenProcessed_thenMaterializesUserView() {
        // Given
        String userId = UUID.randomUUID().toString();
        String dummyUsername = "test-user";
        String dummyEmail = "user@example.com";
        long registeredAt = 1_720_000_000_000L;
        long occurredAt = 1_720_000_001_000L;

        UserRegistered registeredUser = buildUserRegisteredEvent(dummyUsername, dummyEmail, List.of("USER"), registeredAt);
        UserEvent userEvent = buildUserEvent(userId, registeredUser, occurredAt);

        // When
        inputTopic.pipeInput(userId, userEvent);
        UserView result = usersStore.get(userId);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getUserId()).isEqualTo(userId);
        assertThat(result.getUsername()).isEqualTo(dummyUsername);
        assertThat(result.getEmail()).isEqualTo(dummyEmail);
        assertThat(result.getRolesList()).containsExactly("USER");
        assertThat(result.getRegisteredAt()).isEqualTo(registeredAt);
        assertThat(result.getUpdatedAt()).isEqualTo(occurredAt);
    }

    @Test
    void givenEventWithoutSupportedPayload_whenProcessed_thenDoesNotMaterializeUserView() {
        String userId = UUID.randomUUID().toString();
        long occurredAt = 1_720_000_000_000L;

        UserEvent userEvent = buildUserEventWithoutRegisteredPayload(userId, occurredAt);

        inputTopic.pipeInput(userId, userEvent);
        UserView result = usersStore.get(userId);

        assertThat(result).isNull();
    }

    private static UserRegistered buildUserRegisteredEvent(String username, String email, List<String> roles, long registeredAt) {
        return UserRegistered.newBuilder()
            .setUsername(username)
            .setEmail(email)
            .addAllRoles(roles)
            .setRegisteredAt(registeredAt)
            .build();
    }

    private static UserEvent buildUserEvent(String userId, UserRegistered registeredUser, long occurredAt) {
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(userId)
            .setOccurredAt(occurredAt)
            .setUserRegistered(registeredUser)
            .build();
    }

    private static UserEvent buildUserEventWithoutRegisteredPayload(String userId, long occurredAt) {
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(userId)
            .setOccurredAt(occurredAt)
            .build();
    }

    private void setupTopology() {
        KafkaStreamsFactory factory = mock(KafkaStreamsFactory.class);
        ManagedKafkaStreams managedStreams = mock(ManagedKafkaStreams.class);

        when(factory.createStreams(any(KafkaStreamsSpec.class), any(Topology.class)))
            .thenReturn(managedStreams);

        UsersView usersView = new UsersView(factory);
        usersView.init();

        ArgumentCaptor<Topology> topologyCaptor = ArgumentCaptor.forClass(Topology.class);

        verify(factory).createStreams(any(KafkaStreamsSpec.class), topologyCaptor.capture());
        verify(managedStreams).start();

        testDriver = new TopologyTestDriver(topologyCaptor.getValue());
        inputTopic = testDriver.createInputTopic(
            USER_EVENTS,
            Serdes.String().serializer(),
            ProtobufSerde.of(UserEvent.parser()).serializer()
        );

        Map<String, StateStore> stateStores = testDriver.getAllStateStores();

        assertThat(stateStores).hasSize(1);

        String storeName = stateStores.keySet()
            .iterator()
            .next();

        usersStore = testDriver.getKeyValueStore(storeName);
    }

}
