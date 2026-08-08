package com.sap.notification.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import com.sap.authkafkastreams.proto.notification_commands.NotificationCommand;
import com.sap.authkafkastreams.proto.token_session.TokenSessionEvent;
import com.sap.kafka.serializer.ProtobufSerde;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.kafka.streams.KafkaStreamsSpec;
import com.sap.kafka.streams.ManagedKafkaStreams;
import com.sap.lifecycle.ManagedLifecycle;

import static com.sap.authkafkastreams.proto.notification_commands.NotificationType.NOTIFICATION_TYPE_LOGIN;
import static com.sap.authkafkastreams.proto.notification_commands.NotificationType.NOTIFICATION_TYPE_LOGOUT;
import static com.sap.kafka.contract.KafkaTopics.NOTIFICATION_COMMANDS;
import static com.sap.kafka.contract.KafkaTopics.TOKEN_SESSION_EVENTS;
import static java.util.Objects.requireNonNull;

public class TokenSessionNotificationRouter implements ManagedLifecycle {

    private static final String APPLICATION_ID = "auth-token-session-notification-router-v1";
    private static final String BRANCH_PREFIX = "token-session-";
    private static final String CREATED = "created";
    private static final String REVOKED = "revoked";
    private static final String LOGIN_NOTIFICATION_MESSAGE = "New login detected for your account.";
    private static final String LOGOUT_NOTIFICATION_MESSAGE = "You have successfully logged out.";

    private final KafkaStreamsFactory streamsFactory;
    private ManagedKafkaStreams streams;

    public TokenSessionNotificationRouter(KafkaStreamsFactory streamsFactory) {
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

        Produced<String, NotificationCommand> notificationProduced =
            Produced.with(
                Serdes.String(),
                ProtobufSerde.of(NotificationCommand.parser())
            );

        builder.stream(
                TOKEN_SESSION_EVENTS,
                Consumed.with(
                    Serdes.String(),
                    ProtobufSerde.of(TokenSessionEvent.parser())
                )
            )
            .filter(TokenSessionNotificationRouter::isSupportedEvent)
            .split(Named.as(BRANCH_PREFIX))
            .branch(
                (userId, event) -> event.hasTokenSessionCreated(),
                Branched.withConsumer(
                    createdEvents -> createdEvents
                        .mapValues(
                            TokenSessionNotificationRouter::toLoginCommand
                        )
                        .to(
                            NOTIFICATION_COMMANDS,
                            notificationProduced
                        ),
                    CREATED
                )
            )
            .branch(
                (userId, event) -> event.hasTokenSessionRevoked(),
                Branched.withConsumer(
                    revokedEvents -> revokedEvents
                        .mapValues(
                            TokenSessionNotificationRouter::toLogoutCommand
                        )
                        .to(
                            NOTIFICATION_COMMANDS,
                            notificationProduced
                        ),
                    REVOKED
                )
            )
            .noDefaultBranch();

        return builder.build();
    }

    private static boolean isSupportedEvent(
        String userId,
        TokenSessionEvent event
    ) {
        return userId != null
            && !userId.isBlank()
            && event != null
            && !event.getEventId().isBlank()
            && !event.getTokenId().isBlank()
            && userId.equals(event.getUserId())
            && (event.hasTokenSessionCreated()
            || event.hasTokenSessionRevoked());
    }

    private static NotificationCommand toLoginCommand(
        TokenSessionEvent event
    ) {
        return createBaseCommand(event)
            .setNotificationType(NOTIFICATION_TYPE_LOGIN)
            .setMessage(LOGIN_NOTIFICATION_MESSAGE)
            .build();
    }

    private static NotificationCommand toLogoutCommand(
        TokenSessionEvent event
    ) {
        return createBaseCommand(event)
            .setNotificationType(NOTIFICATION_TYPE_LOGOUT)
            .setMessage(LOGOUT_NOTIFICATION_MESSAGE)
            .build();
    }

    private static NotificationCommand.Builder createBaseCommand(
        TokenSessionEvent event
    ) {
        return NotificationCommand.newBuilder()
            .setCommandId("notification-" + event.getEventId())
            .setSourceEventId(event.getEventId())
            .setUserId(event.getUserId())
            .setTokenId(event.getTokenId())
            .setCreatedAt(event.getOccurredAt());
    }
}
