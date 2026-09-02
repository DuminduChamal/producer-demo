package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

// Change vs. SimpleProducer: fires sends asynchronously with a callback
// instead of blocking on .get() per record, so multiple records can
// actually accumulate into a batch before being sent as one request.
// Batching (linger.ms/batch.size below) only has an effect on async sends —
// a sync send-then-block loop never has more than one record in flight.
public class AsyncBatchProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Leader waits for all in-sync replicas to write it
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);       // wait up to 20ms to batch more records
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);   // 32KB batches

        int messageCount = 1000;
        AtomicInteger acked = new AtomicInteger();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("keyed-topic", "batch-key", "message-" + i);

                // Async send: don't block. The callback fires later, on a
                // producer I/O thread, once the broker responds.
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        acked.incrementAndGet();
                    }
                });
            }

            producer.flush(); // block here until everything queued has actually been sent
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("Sent %d messages, %d acked, in %d ms%n",
                    messageCount, acked.get(), elapsed);
        }
    }
}