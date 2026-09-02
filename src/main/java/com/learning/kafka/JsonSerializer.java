package com.learning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

// Generic Serializer<T> used by JsonProducer for any object type, encoding
// via Jackson's ObjectMapper. Implements Kafka's Serializer<T> interface
// directly — the same extension point StringSerializer uses.
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            // Kafka only moves bytes — this is where the object becomes wire format
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON", e);
        }
    }
}