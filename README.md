# Kafka Producer — Learning Project

Hands-on Java examples that build up Apache Kafka producer concepts
incrementally: a minimal single-message send, keyed partitioning, async
batching, retry/durability tuning, and custom (JSON) serialization. Written
while learning Kafka from scratch, each class isolates one concept so its
behavior is easy to observe in isolation before combining them. Companion
project to [`consumer-demo`](https://github.com/DuminduChamal/consumer-demo),
which reads back everything sent here.

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

The broker is a standalone long-running process, independent of anything in
this repo — if it stops (closed terminal, crash), every producer/consumer
loses its connection until it's restarted. Nothing here manages that for
you.

## Topics used by these examples

```bash
bin/kafka-topics.sh --create --topic learning-topic \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin/kafka-topics.sh --create --topic keyed-topic \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

bin/kafka-topics.sh --create --topic orders-topic \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

- `learning-topic` (1 partition) — used only by `SimpleProducer`.
- `keyed-topic` (3 partitions) — used by `KeyedProducer`, `AsyncBatchProducer`,
  and `RetryDemoProducer`, so partition-routing behavior is actually visible.
- `orders-topic` (3 partitions) — used by `JsonProducer` specifically, kept
  separate from `keyed-topic` so it only ever carries JSON messages. A topic
  with mixed message formats (plain strings alongside JSON) breaks a JSON
  consumer outright — it throws on the first non-JSON record instead of
  skipping it. In general, **one topic should carry one consistent message
  schema**; this is exactly the problem a Schema Registry solves in
  production.

All three topics use `replication-factor=1` because this is a single-broker
setup — see the `acks` note under `AsyncBatchProducer` below for why that
matters.

## Running an example

Each class has its own `main`. Point the `exec-maven-plugin` at the one you
want to run by setting `mainClass` in `pom.xml`, then:

```bash
mvn compile exec:java
```

## Examples, in the order they were built

### 1. `SimpleProducer`
The minimal case: configure a `KafkaProducer<String, String>` with
`bootstrap.servers` and a `StringSerializer` for both key and value, build
one `ProducerRecord`, send it, and block on `.get()` to print the partition
and offset the broker assigned.

Serializers exist because Kafka only ever moves raw bytes over the wire — it
has no idea what a "String" or a "user object" is. `StringSerializer` just
UTF-8-encodes the value; every other example in this repo either reuses that
for plain-text payloads or supplies a custom one (see `JsonProducer`).
Calling `.get()` on the `Future` returned by `send()` makes the call
synchronous — useful here to see the send/ack lifecycle clearly, at the cost
of blocking until the broker responds. `AsyncBatchProducer` replaces this
with a non-blocking pattern once that trade-off starts to matter.

### 2. `KeyedProducer`
Sends multiple messages across several keys (`user-1`, `user-2`, `user-3`) to
`keyed-topic`. Demonstrates that Kafka's default partitioner routes by
`hash(key) % numPartitions` (using the `murmur2` hash function) — the same
key always lands on the same partition, deterministically, forever (as long
as the partition count doesn't change — see below).

This is the mechanism behind **per-key ordering**. A Kafka topic is really a
set of independent, unordered-relative-to-each-other partitions; Kafka only
guarantees message order *within* a single partition. Routing all of one
entity's events to the same partition via its key is what gives that
entity's events a consistent order, without needing global ordering across
the whole topic.

Two things worth knowing:

- **Uneven spread with few keys is expected, not a bug.** Hash uniformity is
  a statistical property that only shows up over many keys — with just 3
  keys and 3 partitions, there's a real (~11%) chance they all land on the
  same partition. What actually matters for correctness is that a given key
  is *consistently* routed to the same partition, not that a handful of keys
  spread evenly.
- **Adding partitions later breaks existing key routing.** `hash(key) %
  numPartitions` changes for every key the moment `numPartitions` changes —
  old messages stay where they were written, but new messages for the same
  key can land on a different partition. Partition count is meant to be
  fixed once ordering is being relied on.

### 3. `AsyncBatchProducer`
Fires sends asynchronously with a callback instead of blocking on `.get()`
per message, so multiple records can actually accumulate into a batch before
being sent as one request. Configures:

- **`acks`** — durability vs. latency:
  | Value | Meaning |
  |---|---|
  | `0` | Fire-and-forget, no broker response awaited — fastest, drops messages silently on failure |
  | `1` | Leader broker acknowledges after writing to its own log — leader failure before replication still loses the message |
  | `all` (`-1`) | Leader waits for every in-sync replica to write it — strongest durability, slowest |

  With `replication-factor=1` (as used throughout this repo), the leader
  *is* the only in-sync replica, so `acks=1` and `acks=all` behave
  identically here. The distinction only shows up with `replication-factor
  ≥ 2` on a real multi-broker cluster — still worth setting `acks`
  explicitly so the config is correct once that's true.

- **`linger.ms`** — how long the producer waits for a batch to fill before
  sending it anyway. Default is `0` (send almost immediately, barely any
  batching). Raising it trades a small added per-message latency for far
  fewer, larger requests under load.
- **`batch.size`** — max bytes per partition-batch; a full batch is sent
  immediately regardless of `linger.ms`.

Batching only has an effect on async sends — a sync send-then-block loop
never has more than one record in flight, so there's nothing to batch.

### 4. `RetryDemoProducer`
Shortens `delivery.timeout.ms`, `request.timeout.ms`, and
`retry.backoff.ms` so retry behavior is observable in seconds rather than
the 2-minute default. Meant to be run against a broker you manually stop and
(optionally) restart mid-send.

Not every failure is retried — Kafka distinguishes **retriable** errors
(broker temporarily unreachable, request timeouts, leader election in
progress — transient cluster-state issues) from **non-retriable** ones
(oversized record, serialization failure, authorization error — retrying
wouldn't help since the problem is the request itself). `retries` defaults
to effectively unlimited; what actually bounds the retry loop is
`delivery.timeout.ms` — the total wall-clock budget from `send()` to giving
up, covering every retry attempt. `request.timeout.ms` bounds a single
in-flight request before it's considered failed and retried.

Running this example against a stopped-then-restarted broker shows:

- Retries happening silently in the background (visible as the same
  `WARN ... could not be established` logs you'd see from any disconnected
  client) while the broker is down
- A successful send once the broker comes back within the delivery timeout
- A `TimeoutException` once `delivery.timeout.ms` is exhausted, if it doesn't

Since Kafka 3.0, `enable.idempotence=true` is the default, which is what
makes all this retrying *safe*: each producer gets a unique producer ID and
stamps every record with a per-partition sequence number, so the broker can
recognize and silently discard a duplicate caused by a retried send instead
of appending the record twice.

### 5. `JsonProducer` (with `OrderEvent`, `JsonSerializer`)
Sends actual Java objects instead of strings, to `orders-topic`.
`JsonSerializer<T>` implements Kafka's `Serializer<T>` interface directly —
the same extension point `StringSerializer` uses — and encodes objects to
bytes via Jackson's `ObjectMapper`. Kafka itself ships no JSON serializer;
this is the minimal way to move beyond primitive types, and the type
parameter on `KafkaProducer<String, OrderEvent>` flows through generically —
Kafka doesn't need to know or care that the bytes underneath are JSON.

JSON has no schema enforcement: nothing stops a producer from sending a
field with the wrong type, or silently renaming/dropping a field that
consumers still expect. The production-grade next step is Avro or Protobuf
with a Schema Registry, where producers and consumers validate against a
shared, versioned schema and incompatible changes get rejected at write
time instead of breaking something downstream later.

## Verifying output

Watch messages land in `keyed-topic`, including which partition and key each
one carries:

```bash
bin/kafka-console-consumer.sh --topic keyed-topic --from-beginning \
  --bootstrap-server localhost:9092 \
  --property print.key=true --property print.partition=true --property key.separator=":"
```

For `JsonProducer`, point the same command at `orders-topic` instead — the
raw JSON bytes print as text, since the console consumer has no idea (and
doesn't need to know) what's inside them.

## What's next

Producer-side topics not yet covered here: compression (`compression.type`),
`buffer.memory` / backpressure, transactional/exactly-once producers, and
custom partitioners.

On the consumer side (see [`consumer-demo`](https://github.com/DuminduChamal/consumer-demo)),
everything sent here gets read back with a real `KafkaConsumer`: consumer
groups and rebalancing, manual offset commits (`commitSync`/`commitAsync`)
and the delivery-guarantee trade-offs they control, and a matching
`Deserializer<OrderEvent>` for the JSON messages produced here.
