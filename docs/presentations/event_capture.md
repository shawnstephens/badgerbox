# Event Capture and Cloud Considerations
### Reliable event delivery when the broker is unavailable

---

## A Bit of History
- Began my messaging journey with IBM MQ Series at an ATM company
- Used MSMQ in other applications there
- Moved into the Java world and used JMX
- In late 2008, worked on the Six Flags ticketing system using MSMQ via WCF
- In the 2010s, used RabbitMQ and CloudAMQP heavily
- In 2019, my employer moved from RabbitMQ to Kafka, which introduced very different ordering and partition considerations

---

## The Problem That Never Went Away
- Across almost every messaging system, the same production question kept showing up: what happens when the broker is down and we cannot publish?
- The one major exception was WCF + MSMQ on Windows servers, where a local durable outbox could forward to the remote queue when it became available again.
- The right answer depends on reliability and durability needs.
- This talk focuses on the cases where we need a high probability that no data is lost.

---

## Decision Lens
- Where can I durably persist intent before the broker is reachable?
- Can I capture the event in the same local transaction as the business state change?
- Do I want to pay that cost in my primary database, in a CDC pipeline, or on local disk?
- Which failure domains do I trust most: database, broker, network, sidecar, or local disk?
- No matter which option I choose, consumers should be idempotent.

---

## Option 1: Transactional Outbox
- Write the business state change and the event to the database in the same transaction.
- If the transaction commits, both are durable.
- A relay process reads pending outbox records and publishes them to the broker.
- This is usually the best fit when the endpoint already depends on a database.

![Transactional outbox pattern](https://microservices.io/i/patterns/data/ReliablePublication.png)

Source: https://microservices.io/patterns/data/transactional-outbox

---

## Option 1: Tradeoffs
- I wrote a lot of transactional outboxes, especially in Go.
- PostgreSQL makes this pattern practical with `FOR UPDATE SKIP LOCKED`.
- MySQL 8.x supports `SKIP LOCKED`; MySQL 5.x does not.
- The upside is atomic capture of both state and message.
- The downside is extra database write load, relay complexity, and throughput bounded by the database.

### Practical Postgres note
- [River](https://riverqueue.com/) combines `LISTEN/NOTIFY` pub/sub with `FOR UPDATE SKIP LOCKED` as a fallback.
- [River benchmarks](https://riverqueue.com/docs/benchmarks) report about 46k jobs/sec overall on an 8-core 2022 M2 MacBook Air.

---

## Option 2: Change Data Capture (CDC) with Kafka Connectors
- The application writes once to the database.
- A CDC connector tails the write-ahead log or binlog.
- Kafka Connect publishes those changes to Kafka topics.
- This works well when the database is the source of truth and Kafka is the distribution layer.

![Debezium CDC architecture](https://debezium.io/documentation/reference/_images/debezium-architecture.png)

Source: https://debezium.io/documentation/reference/architecture.html

---

## Option 2: CDC Pros and Cons

### Pros
- Avoids dual-write logic in application code.
- Low-latency capture from the database log.
- Strong fit for fan-out into Kafka, analytics, search, and downstream systems.
- Keeps the database as the system of record.

### Cons
- Adds operational complexity: Kafka Connect, connector configs, monitoring, re-snapshotting, schema drift.
- Raw table changes are not always the same thing as intentional domain events.
- Deletes, ordering, and schema evolution still need deliberate handling.
- In many cases, the cleanest CDC design is CDC over an explicit outbox table, not over arbitrary business tables.

---

## Option 3: Write-Ahead Logs / Disk-Based Outbox
- Sometimes there is no Postgres, no MySQL 8.x, or no desire to put more load on a shared database.
- In those cases, publishing straight to the broker is fine until the broker is unavailable.
- The fallback is a durable local outbox on disk.
- An embedded processor drains that local backlog when the broker recovers.
- This reduces dependence on sidecars and avoids introducing another moving part that can fail.

---

## Why Not Just Use `epilog`?
- Historically, the logging Kafka pipeline has not had the same durability bar as our transactional databases.
- There have been multiple postmortems involving both `epilog` and the Kafka cluster dominated by logging traffic.
- Noisy-neighbor and overloaded producer behavior have been major contributors in those cases.
- That tradeoff made sense for logs, because logs are useful, but they are not usually must-not-lose business data.
- Cost pressure on high-volume logging pipelines also limited investments like aggressive replication.

---

## Cloud Reality Is Improving, But Path Still Matters
- AZW and AZE now separate application clusters from logging clusters.
- The application cluster is configured with replication.
- If that replication is not cross-DC yet, it likely will be soon.
- `epilog` is not multi-sync today, so anything using `epilog` still produces to the logging cluster.
- The cloud architecture is improving, but the durability profile still depends on which pipeline I choose.

---

## Enter BadgerBox
- `badgerbox` is an embedded durable outbox for Go applications that already use Badger.
- It stores typed payloads and typed destinations locally, then runs a processor with retries, lease recovery, and a dead-letter queue.
- It gives me a disk-backed option when I want durable capture without coupling everything to Postgres or to a sidecar.
- The Kafka adapter lets it publish to Kafka once the broker is healthy again.

![BadgerBox](../../image.png)

Repo: https://github.com/shawnstephens/badgerbox

---

## Choosing the Pattern
- If the request path already writes to Postgres or MySQL 8.x, start with a transactional outbox.
- If the database is the source of truth and the platform already runs Kafka Connect, CDC is a strong option.
- If I need embedded durability with minimal external dependencies, use a disk-based outbox.
- In every model, assume retries will happen and keep consumers idempotent.
- The main design question is simple: where do I want to durably capture intent before the broker is available again?

---

## Sources
- Transactional outbox pattern and image: https://microservices.io/patterns/data/transactional-outbox
- Debezium CDC architecture and Kafka Connect model: https://debezium.io/documentation/reference/architecture.html
- River benchmark reference: https://riverqueue.com/docs/benchmarks
- BadgerBox repo: https://github.com/shawnstephens/badgerbox
