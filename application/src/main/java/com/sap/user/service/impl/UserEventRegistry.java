package com.sap.user.service.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.protobuf.Message;
import com.sap.authkafkastreams.proto.user.UserEvent;
import com.sap.authkafkastreams.proto.user.UserRegistered;
import com.sap.kafka.producer.IKafkaPublisher;
import com.sap.user.domain.User;
import com.sap.user.service.IUserEventRegistry;

import static com.sap.kafka.contract.KafkaTopics.USER_EVENTS;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public class UserEventRegistry implements IUserEventRegistry {

    private final IKafkaPublisher<String, Message> publisher;

    public UserEventRegistry(IKafkaPublisher<String, Message> publisher) {
        this.publisher = requireNonNull(publisher, "Kafka publisher must not be null");
    }

    @Override
    public CompletableFuture<RecordMetadata> publishUserRegisteredEvent(User user) {
        UserEvent userEvent = createUserEvent(user);

        return publisher.publish(USER_EVENTS, userEvent.getUserId(), userEvent);
    }

    private static UserEvent createUserEvent(User user) {
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(user.id().toString())
            .setOccurredAt(now().toEpochMilli())
            .setUserRegistered(createUserRegisteredEvent(user))
            .build();
    }

    private static UserRegistered createUserRegisteredEvent(User user) {
        return UserRegistered.newBuilder()
            .setUsername(user.username())
            .setEmail(user.email())
            .addAllRoles(user.roles().stream()
                .map(Enum::name)
                .toList()
            )
            .setRegisteredAt(user.createdAt().toEpochMilli())
            .build();
    }
}
