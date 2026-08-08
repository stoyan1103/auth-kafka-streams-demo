package com.sap.security.jwt.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.sap.security.jwt.JwtTokenSession;

public interface ITokenSessionEventRegistry {

    CompletableFuture<RecordMetadata> publishTokenSessionCreatedEvent(JwtTokenSession session);

    CompletableFuture<RecordMetadata> publishTokenSessionRevokedEvent(UUID userId, String tokenId);
}
