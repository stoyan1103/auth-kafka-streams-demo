package com.sap.kafka.contract;

public final class KafkaTopics {

    public static final String USER_EVENTS = "user.events.v1";
    public static final String TOKEN_SESSION_EVENTS = "token.session.events.v1";
    public static final String NOTIFICATION_COMMANDS = "notification.commands.v1";
    public static final String NOTIFICATION_STATUS = "notification.status.v1";
    public static final String USER_AUDIT_EVENTS = "user.audit.events.v1";

    private KafkaTopics() {}
}

