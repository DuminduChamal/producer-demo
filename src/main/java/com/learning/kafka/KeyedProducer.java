package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

// Change vs. SimpleProducer: sends multiple keyed messages instead of one
// unkeyed message. Demonstrates that the default partitioner routes by
// hash(key) % numPartitions — same key always lands on the same partition,
// which is the mechanism behind per-key ordering (Kafka only guarantees
// order within a partition, not across a whole topic).
public class KeyedProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String[] userIds = {"user-1", "user-2", "user-3"};

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Send 2 messages per user. Same key -> same partition, every time.
            for (int i = 0; i < 2; i++) {
                for (String userId : userIds) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("keyed-topic", userId, "event-" + i + " from " + userId);

                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("key=%s -> partition=%d offset=%d%n",
                            userId, metadata.partition(), metadata.offset());
                }
            }
        }
    }
}