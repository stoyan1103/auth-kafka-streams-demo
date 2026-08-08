package com.sap.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.Message;

public class ProtobufSerializer<T extends Message> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();
    }
}

