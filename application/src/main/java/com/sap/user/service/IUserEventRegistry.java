package com.sap.user.service;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

import com.sap.user.domain.User;

public interface IUserEventRegistry {

    CompletableFuture<RecordMetadata> publishUserRegisteredEvent(User user);
}
