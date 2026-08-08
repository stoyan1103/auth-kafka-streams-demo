package com.sap.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import static java.util.Objects.requireNonNull;

public class ProtobufSerde<T extends Message> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    private ProtobufSerde(Parser<T> parser) {
        this.serializer = new ProtobufSerializer<>();
        this.deserializer = new ProtobufDeserializer<>(parser);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    public static <T extends Message> ProtobufSerde<T> of(Parser<T> parser) {
        return new ProtobufSerde<>(requireNonNull(parser, "parser must not be null"));
    }
}
