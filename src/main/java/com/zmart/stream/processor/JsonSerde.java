package com.zmart.stream.processor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;
    private static ObjectMapper objectMapper = new ObjectMapper();

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return objectMapper.readValue(bytes, type);
            } catch (IOException e) {
                throw new SerializationException("Cannot deserialize from topic " + topic, e);
            }
        };
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new SerializationException("Cannot serialize for topic " + topic, e);
            }
        };
    }
    
}
