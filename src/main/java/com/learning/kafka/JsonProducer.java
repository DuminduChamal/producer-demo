package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class JsonProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Value serializer is now OUR class, not Kafka's built-in one
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props)) {
            OrderEvent order = new OrderEvent("order-123", 49.99, System.currentTimeMillis());

            ProducerRecord<String, OrderEvent> record =
                    new ProducerRecord<>("orders-topic", order.getOrderId(), order);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent %s -> partition=%d offset=%d%n", order, metadata.partition(), metadata.offset());
        }
    }
}