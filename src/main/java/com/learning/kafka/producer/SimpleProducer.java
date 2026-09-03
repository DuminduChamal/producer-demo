package com.learning.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
//import java.util.concurrent.atomic.AtomicInteger;

// Baseline producer: the simplest possible send — one record, synchronous
// (.get() blocks until acked), default acks/retries/batching. Every other
// producer in this project is a variation that changes exactly one thing
// relative to this one — async sends, batching config, retry behavior, or
// serialization — so this is the reference point to compare them against.
public class SimpleProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Kafka sends raw bytes over the wire — serializers convert your
        // key/value objects (here, Strings) into those bytes.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("learning-topic", "Hello Kafka!");

            // .get() blocks until the broker acknowledges the send,
            // so we can see exactly where the message landed.
            RecordMetadata metadata = producer.send(record).get();

            System.out.printf("Sent to topic=%s partition=%d offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }

        // This is to see timing with sync message processing using .get()
//        int messageCount = 1000;
//        AtomicInteger acked = new AtomicInteger();
//
//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//            long start = System.currentTimeMillis();
//
//            for (int i = 0; i < messageCount; i++) {
//                ProducerRecord<String, String> record =
//                        new ProducerRecord<>("learning-topic", "Hello Kafka!");
//
//                producer.send(record, (metadata, exception) -> {
//                    if (exception != null) {
//                        exception.printStackTrace();
//                    } else {
//                        acked.incrementAndGet();
//                    }
//                }).get();
//            }
//
//            producer.flush(); // block here until everything queued has actually been sent
//            long elapsed = System.currentTimeMillis() - start;
//
//            System.out.printf("Sent %d messages, %d acked, in %d ms%n",
//                    messageCount, acked.get(), elapsed);
//        }
    }
}