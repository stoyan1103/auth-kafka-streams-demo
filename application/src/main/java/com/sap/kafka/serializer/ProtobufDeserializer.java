package com.sap.kafka.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtobufDeserializer<T extends Message> implements Deserializer<T> {

    private final Parser<T> parser;

    public ProtobufDeserializer(Parser<T> parser) {
        this.parser = requireNonNull(parser, "parser must not be null");
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException(format("Failed to deserialize protobuf message from topic [%s]", topic), e);
        }
    }
}
