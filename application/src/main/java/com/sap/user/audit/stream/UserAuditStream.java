package com.sap.user.audit.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import com.sap.authkafkastreams.proto.token_session.TokenSessionEvent;
import com.sap.authkafkastreams.proto.user.UserEvent;
import com.sap.authkafkastreams.proto.user_audit.UserAuditAction;
import com.sap.authkafkastreams.proto.user_audit.UserAuditEvent;
import com.sap.kafka.serializer.ProtobufSerde;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.kafka.streams.KafkaStreamsSpec;
import com.sap.kafka.streams.ManagedKafkaStreams;
import com.sap.lifecycle.ManagedLifecycle;

import static com.sap.authkafkastreams.proto.user_audit.UserAuditAction.USER_AUDIT_ACTION_LOGGED_IN;
import static com.sap.authkafkastreams.proto.user_audit.UserAuditAction.USER_AUDIT_ACTION_LOGGED_OUT;
import static com.sap.authkafkastreams.proto.user_audit.UserAuditAction.USER_AUDIT_ACTION_REGISTERED;
import static com.sap.kafka.contract.KafkaTopics.TOKEN_SESSION_EVENTS;
import static com.sap.kafka.contract.KafkaTopics.USER_AUDIT_EVENTS;
import static com.sap.kafka.contract.KafkaTopics.USER_EVENTS;
import static java.util.Objects.requireNonNull;

public class UserAuditStream implements ManagedLifecycle {

    private static final String APPLICATION_ID = "auth-user-audit-streams-v1";

    private final KafkaStreamsFactory streamsFactory;
    private ManagedKafkaStreams streams;

    public UserAuditStream(KafkaStreamsFactory streamsFactory) {
        this.streamsFactory = requireNonNull(streamsFactory, "streamsFactory must not be null");
    }

    @Override
    public void init() {
        streams = streamsFactory.createStreams(KafkaStreamsSpec.of(APPLICATION_ID), buildTopology());
        streams.start();
    }

    @Override
    public void destroy() {
        if (streams != null) {
            streams.stop();
        }
    }

    private static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Produced<String, UserAuditEvent> auditProduced =
            Produced.with(
                Serdes.String(),
                ProtobufSerde.of(UserAuditEvent.parser())
            );

        addUserEventsStream(builder, auditProduced);
        addTokenSessionEventsStream(builder, auditProduced);

        return builder.build();
    }

    private static void addUserEventsStream(StreamsBuilder builder, Produced<String, UserAuditEvent> auditProduced) {
        builder.stream(
                USER_EVENTS,
                Consumed.with(
                    Serdes.String(),
                    ProtobufSerde.of(UserEvent.parser())
                )
            )
            .filter(UserAuditStream::isSupportedUserEvent)
            .mapValues(UserAuditStream::toRegisteredAuditEvent)
            .to(USER_AUDIT_EVENTS, auditProduced);
    }

    private static void addTokenSessionEventsStream(StreamsBuilder builder, Produced<String, UserAuditEvent> auditProduced) {
        builder.stream(
                TOKEN_SESSION_EVENTS,
                Consumed.with(
                    Serdes.String(),
                    ProtobufSerde.of(TokenSessionEvent.parser())
                )
            )
            .filter(UserAuditStream::isSupportedTokenSessionEvent)
            .mapValues(UserAuditStream::toTokenSessionAuditEvent)
            .to(USER_AUDIT_EVENTS, auditProduced);
    }

    private static boolean isSupportedUserEvent(String userId, UserEvent event) {
        return userId != null
            && !userId.isBlank()
            && event != null
            && !event.getEventId().isBlank()
            && userId.equals(event.getUserId())
            && event.hasUserRegistered();
    }

    private static boolean isSupportedTokenSessionEvent(String userId, TokenSessionEvent event) {
        return userId != null
            && !userId.isBlank()
            && event != null
            && !event.getEventId().isBlank()
            && !event.getTokenId().isBlank()
            && userId.equals(event.getUserId())
            && (event.hasTokenSessionCreated()
            || event.hasTokenSessionRevoked());
    }

    private static UserAuditEvent toRegisteredAuditEvent(UserEvent event) {
        return createBaseAuditEvent(
            event.getEventId(),
            event.getUserId(),
            event.getOccurredAt()
        )
            .setAction(USER_AUDIT_ACTION_REGISTERED)
            .build();
    }

    private static UserAuditEvent toTokenSessionAuditEvent(TokenSessionEvent event) {
        UserAuditAction action = event.hasTokenSessionCreated() ? USER_AUDIT_ACTION_LOGGED_IN : USER_AUDIT_ACTION_LOGGED_OUT;

        return createBaseAuditEvent(
            event.getEventId(),
            event.getUserId(),
            event.getOccurredAt()
        )
            .setAction(action)
            .setTokenId(event.getTokenId())
            .build();
    }

    private static UserAuditEvent.Builder createBaseAuditEvent(String sourceEventId, String userId, long occurredAt) {
        return UserAuditEvent.newBuilder()
            .setEventId("audit-" + sourceEventId)
            .setSourceEventId(sourceEventId)
            .setUserId(userId)
            .setOccurredAt(occurredAt);
    }
}
