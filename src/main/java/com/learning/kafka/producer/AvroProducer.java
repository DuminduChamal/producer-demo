package com.learning.kafka.producer;

import com.learning.kafka.OrderEventAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

// Change vs. JsonProducer: the value serializer is Confluent's
// KafkaAvroSerializer instead of our hand-rolled JsonSerializer. It needs
// schema.registry.url because, unlike JsonSerializer (which just writes raw
// bytes with no external coordination), Avro serialization registers the
// schema with Schema Registry on first send and validates against it on
// every send after that.
public class AvroProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, OrderEventAvro> producer = new KafkaProducer<>(props)) {
            // Builder pattern comes from the generated class, not something
            // we wrote — same fields as the hand-written OrderEvent POJO.
            OrderEventAvro order = OrderEventAvro.newBuilder()
                    .setOrderId("order-avro-1")
                    .setAmount(75.50)
                    .setTimestamp(System.currentTimeMillis())
                    .setCustomerId("cust-01")
                    .build();

            ProducerRecord<String, OrderEventAvro> record =
                    new ProducerRecord<>("avro-orders-topic", order.getOrderId(), order);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent %s -> partition=%d offset=%d%n", order, metadata.partition(), metadata.offset());
        }
    }
}