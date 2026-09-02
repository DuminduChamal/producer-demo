package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

// Change vs. SimpleProducer: shortens delivery.timeout.ms/request.timeout.ms/
// retry.backoff.ms so retry behavior is observable in seconds instead of the
// 2-minute default. Meant to be run against a broker you manually stop and
// (optionally) restart mid-send, to watch retries happen silently in the
// background and either succeed once the broker returns or time out.
// Retries are safe from duplication because enable.idempotence=true is the
// default since Kafka 3.0 — the broker dedupes by producer ID + sequence
// number rather than appending the same record twice.
public class RetryDemoProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Shortened so we don't have to wait 2 minutes to see the outcome.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);   // give up after 20s total
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);     // each attempt waits up to 5s
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);       // 1s between attempts

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("keyed-topic", "retry-key", "will this survive an outage?");

            long start = System.currentTimeMillis();
            producer.send(record, (metadata, exception) -> {
                long elapsed = System.currentTimeMillis() - start;
                if (exception != null) {
                    System.out.printf("FAILED after %d ms: %s%n", elapsed, exception);
                } else {
                    System.out.printf("SUCCEEDED after %d ms -> partition=%d offset=%d%n",
                            elapsed, metadata.partition(), metadata.offset());
                }
            });

            producer.flush(); // wait here for the callback to fire before exiting
        }
    }
}