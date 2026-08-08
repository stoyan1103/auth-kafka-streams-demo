package com.sap.security.jwt.service.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.protobuf.Message;
import com.sap.authkafkastreams.proto.token_session.TokenSessionCreated;
import com.sap.authkafkastreams.proto.token_session.TokenSessionEvent;
import com.sap.authkafkastreams.proto.token_session.TokenSessionRevoked;
import com.sap.kafka.producer.IKafkaPublisher;
import com.sap.security.jwt.JwtTokenSession;
import com.sap.security.jwt.service.ITokenSessionEventRegistry;

import static com.sap.authkafkastreams.proto.token_session.TokenSessionRevocationReason.TOKEN_SESSION_REVOCATION_REASON_LOGOUT;
import static com.sap.kafka.contract.KafkaTopics.TOKEN_SESSION_EVENTS;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class TokenSessionEventRegistry implements ITokenSessionEventRegistry {

    private final IKafkaPublisher<String, Message> publisher;

    public TokenSessionEventRegistry(IKafkaPublisher<String, Message> publisher) {
        this.publisher = requireNonNull(publisher, "Kafka publisher must not be null");
    }

    @Override
    public CompletableFuture<RecordMetadata> publishTokenSessionCreatedEvent(JwtTokenSession session) {
        requireNonNull(session, "JwtTokenSession must not be null");

        TokenSessionCreated createdEvent = TokenSessionCreated.newBuilder()
            .setExpiresAt(session.expiresAt().toEpochMilli())
            .build();

        TokenSessionEvent event = createBaseEvent(session.userId(), session.tokenId())
            .setTokenSessionCreated(createdEvent)
            .build();

        return publisher.publish(TOKEN_SESSION_EVENTS, session.userId().toString(), event);
    }

    @Override
    public CompletableFuture<RecordMetadata> publishTokenSessionRevokedEvent(UUID userId, String tokenId) {
        requireNonNull(userId, "userId must not be null");
        requireNonNull(tokenId, "tokenId must not be null");

        TokenSessionRevoked revokedEvent = TokenSessionRevoked.newBuilder()
            .setReason(TOKEN_SESSION_REVOCATION_REASON_LOGOUT)
            .build();

        TokenSessionEvent event = createBaseEvent(userId, tokenId)
            .setTokenSessionRevoked(revokedEvent)
            .build();

        return publisher.publish(
            TOKEN_SESSION_EVENTS,
            userId.toString(),
            event
        );

    }

    private static TokenSessionEvent.Builder createBaseEvent(UUID userId, String tokenId) {
        return TokenSessionEvent.newBuilder()
            .setEventId(randomUUID().toString())
            .setUserId(userId.toString())
            .setTokenId(tokenId)
            .setOccurredAt(now().toEpochMilli());
    }
}
