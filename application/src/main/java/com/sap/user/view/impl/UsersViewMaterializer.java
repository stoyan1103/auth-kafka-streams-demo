package com.sap.user.view.impl;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.sap.authkafkastreams.proto.user.UserEvent;
import com.sap.authkafkastreams.proto.user.UserRegistered;
import com.sap.authkafkastreams.proto.user.UserView;

import static java.util.Objects.requireNonNull;

public class UsersViewMaterializer implements Processor<String, UserEvent, Void, Void> {

    private final String storeName;
    private KeyValueStore<String, UserView> store;

    public UsersViewMaterializer(String storeName) {
        this.storeName = requireNonNull(storeName, "storeName must not be null");
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<String, UserEvent> record) {
        UserEvent userEvent = record.value();
        if (!userEvent.hasUserRegistered()) {
            return;
        }

        UserRegistered registered = userEvent.getUserRegistered();

        UserView userView = UserView.newBuilder()
            .setUserId(userEvent.getUserId())
            .setUsername(registered.getUsername())
            .setEmail(registered.getEmail())
            .addAllRoles(registered.getRolesList())
            .setRegisteredAt(registered.getRegisteredAt())
            .setUpdatedAt(userEvent.getOccurredAt())
            .build();

        store.put(userEvent.getUserId(), userView);
    }
}
