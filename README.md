# Kafka Producer — Learning Project

A set of hands-on Java examples that build up Apache Kafka producer concepts
incrementally: a minimal single-message send, keyed partitioning, batching
and retry tuning, and custom (JSON) serialization. Written while learning
Kafka from scratch, each class isolates one concept so the behavior is easy
to observe on its own.

## Prerequisites

- **Java 17+** — required by the Kafka 4.x broker itself. The `kafka-clients`
  library used by this project only requires Java 11+, but you'll need 17+
  on the machine running the broker.
- **Maven**
- **A local Kafka 4.3.1 broker**, running in KRaft mode (no ZooKeeper) at
  `localhost:9092`. This repo contains only the producer code — the broker
  is expected to run separately, from a standalone Kafka download.

## Setting up the broker (once)

Download Kafka 4.3.1 from the [official downloads page](https://kafka.apache.org/community/downloads/),
extract it, then format storage and start the broker in standalone KRaft mode:

```bash
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

Note: Kafka 4.x moved the KRaft config to `config/server.properties` directly
(older tutorials reference `config/kraft/server.properties`, which no longer
exists — KRaft is now the only mode, so the separate subfolder was dropped).

## Topics used by these examples

```bash
bin/kafka-topics.sh --create --topic learning-topic \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin/kafka-topics.sh --create --topic keyed-topic \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

`learning-topic` is used by the very first example; everything after that
uses `keyed-topic` (3 partitions) so partition-routing behavior is actually
visible.

## Running an example

Each class has its own `main`. Point the `exec-maven-plugin` at the one you
want to run by setting `mainClass` in `pom.xml`, then:

```bash
mvn compile exec:java
```

## Examples, in the order they were built

### 1. `SimpleProducer`
The minimal case: configure a `KafkaProducer<String, String>`, build one
`ProducerRecord`, send it, and block on `.get()` to print the partition and
offset the broker assigned. Good for seeing the send/ack lifecycle before
anything else is layered on.

### 2. `KeyedProducer`
Sends multiple messages across several keys (`user-1`, `user-2`, `user-3`) to
`keyed-topic`. Demonstrates that Kafka's default partitioner routes by
`hash(key) % numPartitions` — the same key always lands on the same
partition. This is the mechanism behind **per-key ordering**: Kafka only
guarantees message order *within* a partition, so routing all of one
entity's events to a single partition is what gives that entity's events a
consistent order. Note: with a small number of distinct keys, an even spread
across partitions isn't guaranteed — hash collisions with few samples are
statistically unremarkable. What matters is that a given key is
*consistently* routed to the same partition.

### 3. `AsyncBatchProducer`
Fires sends asynchronously with a callback (instead of blocking on `.get()`
per message) so multiple records can actually accumulate into a batch. Tunes:

- `acks` — durability vs. latency (`0` / `1` / `all`)
- `linger.ms` — how long to wait for a batch to fill before sending anyway
- `batch.size` — max batch size in bytes before it's sent regardless of `linger.ms`

Batching only has an effect on async sends — a sync send-then-block loop
never has more than one record in flight, so there's nothing to batch.

### 4. `RetryDemoProducer`
Shortens `delivery.timeout.ms`, `request.timeout.ms`, and
`retry.backoff.ms` so retry behavior is observable in seconds rather than
the 2-minute default. Meant to be run against a broker you manually stop and
(optionally) restart mid-send, to see:

- Retries happening silently against a temporarily unavailable broker
- A successful send once the broker comes back within the delivery timeout
- A `TimeoutException` once `delivery.timeout.ms` is exhausted, if it doesn't

Since Kafka 3.0, `enable.idempotence=true` is the default, so retries are
safe from duplication — the broker deduplicates by producer ID + per-partition
sequence number rather than appending the same record twice.

### 5. `JsonProducer` (with `OrderEvent`, `JsonSerializer`)
Sends actual Java objects instead of strings. `JsonSerializer<T>` implements
Kafka's `Serializer<T>` interface directly (the same extension point
`StringSerializer` uses) and encodes objects to bytes via Jackson. Kafka
itself has no built-in JSON serializer — this is the minimal way to move
beyond primitive types. For schema enforcement in production, the next step
beyond this is Avro/Protobuf with a Schema Registry, which validates
producer/consumer schemas at write time instead of failing silently
downstream.

## Verifying output

Watch messages land in `keyed-topic`, including which partition and key each
one carries:

```bash
bin/kafka-console-consumer.sh --topic keyed-topic --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true --property print.partition=true --property key.separator=":"
```

## What's next

Producer-side topics not yet covered here: compression (`compression.type`),
`buffer.memory` / backpressure, transactional/exactly-once producers, and
custom partitioners. The natural next step after the producer side is
writing a real `KafkaConsumer` in Java to replace the CLI console consumer
used throughout these examples.
