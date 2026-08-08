package com.sap.notification.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import com.sap.authkafkastreams.proto.notification.NotificationStatusEvent;
import com.sap.authkafkastreams.proto.notification_commands.NotificationCommand;
import com.sap.kafka.serializer.ProtobufSerde;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.kafka.streams.KafkaStreamsSpec;
import com.sap.kafka.streams.ManagedKafkaStreams;
import com.sap.lifecycle.ManagedLifecycle;

import static com.sap.authkafkastreams.proto.notification.NotificationDeliveryStatus.NOTIFICATION_DELIVERY_STATUS_DELIVERED;
import static com.sap.kafka.contract.KafkaTopics.NOTIFICATION_COMMANDS;
import static com.sap.kafka.contract.KafkaTopics.NOTIFICATION_STATUS;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.KeyValue.pair;

public class NotificationDeliverySimulator implements ManagedLifecycle {

    private static final String APPLICATION_ID = "auth-notification-delivery-simulator-v1";

    private final KafkaStreamsFactory streamsFactory;
    private ManagedKafkaStreams streams;

    public NotificationDeliverySimulator(KafkaStreamsFactory streamsFactory) {
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

        builder.stream(
                NOTIFICATION_COMMANDS,
                Consumed.with(Serdes.String(), ProtobufSerde.of(NotificationCommand.parser())
                )
            )
            .filter(NotificationDeliverySimulator::isValidCommand)
            .map((userId, command) -> pair(command.getCommandId(), toDeliveredStatus(command)))
            .to(NOTIFICATION_STATUS,
                Produced.with(Serdes.String(),
                    ProtobufSerde.of(NotificationStatusEvent.parser()))
            );

        return builder.build();
    }

    private static boolean isValidCommand(String userId, NotificationCommand command) {
        return userId != null
            && !userId.isBlank()
            && command != null
            && !command.getCommandId().isBlank()
            && !command.getUserId().isBlank()
            && userId.equals(command.getUserId());
    }

    private static NotificationStatusEvent toDeliveredStatus(NotificationCommand command) {
        return NotificationStatusEvent.newBuilder()
            .setEventId("status-" + command.getCommandId())
            .setCommandId(command.getCommandId())
            .setUserId(command.getUserId())
            .setStatus(NOTIFICATION_DELIVERY_STATUS_DELIVERED)
            .setOccurredAt(now().toEpochMilli())
            .build();
    }
}
