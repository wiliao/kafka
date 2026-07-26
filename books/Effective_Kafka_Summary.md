# Effective Kafka — Summary of Core Concepts, Patterns, Use Cases & Best Practices

> Based on **Effective Kafka** by _Emil Koutanov_ (Leanpub, 2021)

---

## PART I: FOUNDATIONS

### 1. Event-Driven Architecture (EDA)

**Core tenets:**

- **Emitters** (producers) are unaware of downstream **consumers** — every transmission is a blind broadcast.
- **Event notifications** are immutable — once emitted, an event cannot be modified.
- **Channels** (brokers) decouple emitters from consumers.
- Systems consist of emitters, consumers, and channels; elements may combine roles (acting as both emitter and consumer in a pipeline).

**EDA vs alternatives:**

| Approach                               | Coupling       | Scalability     | Resilience              |
| -------------------------------------- | -------------- | --------------- | ----------------------- |
| Monolith                               | Self-contained | None (vertical) | Single point of failure |
| API integration (distributed monolith) | Tight          | Limited         | Cascading failures      |
| Data decapsulation (read others' DB)   | Tightest       | None            | Brittle                 |
| Shared datastore                       | Tight          | Limited         | Shared fate             |
| **EDA / Event streaming**              | **Loose**      | **Linear**      | **Isolated failures**   |

**When EDA fits:**

- Asynchronous event notification and reaction
- Multiple independent consumers of the same data
- Systems requiring loose coupling and independent evolution

**When EDA does NOT fit:**

- Synchronous request-response interactions
- Simple CRUD applications
- Systems requiring strong transactional guarantees across domains

### 2. Event Streaming vs Message Queues

| Aspect               | Event Streaming (Kafka)               | Traditional MQ                      |
| -------------------- | ------------------------------------- | ----------------------------------- |
| Records are consumed | No — retained for configurable period | Yes — removed after consumption     |
| Ordering             | Total order within a partition        | Typically FIFO per queue            |
| Consumers            | Multiple independent groups           | Competing consumers                 |
| Throughput           | Millions/sec on commodity hardware    | Lower                               |
| Persistence          | Durable commit log                    | Often ephemeral without persistence |

### 3. Apache Kafka Overview

**What Kafka provides:**

1. **Publish & subscribe** to streams of events
2. **Store** event streams durably and reliably
3. **Process** streams as they occur or retrospectively

**Origins:** Developed at LinkedIn, open-sourced 2011. Named after Franz Kafka — "a system optimised for writing."

---

## PART II: CORE CONCEPTS

### 4. Architecture Components

| Component          | Role                                                                         |
| ------------------ | ---------------------------------------------------------------------------- |
| **Broker**         | Kafka server; persists and serves records                                    |
| **Controller**     | Manages partition leaders and metadata via Raft quorum (KRaft)               |
| **ZooKeeper**      | _(Legacy, removed in Kafka 4.0)_ Managed cluster metadata; replaced by KRaft |
| **Producer**       | Client application that publishes records                                    |
| **Consumer**       | Client application that reads records                                        |
| **Consumer Group** | Load-balanced set of consumers sharing partition assignments                 |

### 5. Records

A record is the elemental unit of persistence in Kafka:

| Attribute     | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| **Key**       | Optional classifier; used for partition assignment via hashing |
| **Value**     | Informational payload — the business content                   |
| **Headers**   | Optional metadata key-value pairs                              |
| **Partition** | Zero-based index (can be auto-assigned)                        |
| **Offset**    | 64-bit signed integer; primary key within a partition          |
| **Timestamp** | Millisecond-precise; set by producer or broker                 |

> **Important:** A record's key is NOT a primary key. The true primary key is `(partition, offset)`. Keys are classifiers for grouping related records.

### 6. Topics and Partitions

- **Topic:** A logical aggregation of partitions (exhibits **partial order**)
- **Partition:** A totally ordered, append-only commit log (exhibits **total order**)
- Partitions enable **parallelism** — each partition can be consumed by at most one consumer in a group
- Records with the same key always go to the same partition (deterministic hashing with murmur2)

**Key ordering concepts:**

- **Total order:** Every element has a defined predecessor-successor relationship (within a partition)
- **Partial order:** Some elements are comparable, others are not (across partitions in a topic)
- **Causal order:** Events bound by a happened-before relationship

### 7. Producers

**Send methods:**

1. **Fire-and-forget** — no confirmation
2. **Synchronous** — `Future.get()` to wait for acknowledgment
3. **Asynchronous** — callback on success/failure

**Critical producer configurations:**

| Property                                | Recommendation              | Why                              |
| --------------------------------------- | --------------------------- | -------------------------------- |
| `acks=all`                              | Always for durability       | Waits for all in-sync replicas   |
| `enable.idempotence=true`               | Always                      | Prevents duplicates from retries |
| `max.in.flight.requests.per.connection` | ≤5 with idempotence         | Ordering guarantee               |
| `delivery.timeout.ms`                   | Set based on latency budget | Upper bound on publish time      |
| `compression.type`                      | `snappy`, `lz4`, or `zstd`  | Reduces network and storage      |

### 8. Consumers

**Two consumption modes:**

| Mode               | Method        | Group required? | Auto-rebalance? | Offset persistence  |
| ------------------ | ------------- | --------------- | --------------- | ------------------- |
| **Consumer group** | `subscribe()` | Yes             | Yes             | Kafka-managed       |
| **Free consumer**  | `assign()`    | No              | No              | Application-managed |

**Consumer Groups and Rebalancing:**

- **Eager rebalance:** All consumers stop, partitions revoked, then reassigned
- **Cooperative rebalance (incremental):** Only a subset of partitions reassigned; others continue processing
- **New Consumer protocol (KIP-848):** GA since Kafka 4.0; fully incremental, server-side assignors, no global barrier. Enable with `group.protocol=consumer`.
- **Static group membership** (`group.instance.id`): Consumers keep assignments across restarts (ideal for Kubernetes)

**Critical consumer configurations:**

| Property                         | Recommendation               | Why                                      |
| -------------------------------- | ---------------------------- | ---------------------------------------- |
| `auto.offset.reset=earliest`     | Prefer over default `latest` | Maintains at-least-once semantics        |
| `enable.auto.commit=false`       | Manual commits               | Tighter control over delivery guarantees |
| `group.id`                       | Required for `subscribe()`   | Consumer group identifier                |
| `isolation.level=read_committed` | For transactional consumers  | Hides uncommitted records                |

### 9. Brokers and Clusters

- **Leader replica:** Handles all produce/fetch requests for a partition
- **Follower replicas:** Replicate from the leader; promoted if leader fails
- **In-Sync Replicas (ISR):** Replicas caught up with the leader — only ISR members are eligible for leadership
- `replica.lag.time.max.ms`: Controls ISR membership (default: 30s)
- **Preferred leader:** Original leader when topic was created; used for load balancing

### 10. KRaft (Kafka's Raft-Based Controller)

- KRaft became **production-ready** in Kafka 3.3
- ZooKeeper mode was **removed** in Kafka 4.0+
- Controller nodes form a **Raft quorum** managing the metadata log
- `process.roles` configures servers as `broker`, `controller`, or `broker,controller` (combined)
- Recommended: 3 or 5 dedicated controllers for production
- Dynamic quorum support (Kafka 4.1+): add/remove controllers at runtime

### 11. Data Retention

Kafka provides durable storage with configurable retention:

| Policy                           | Behaviour                                         | Use Case                        |
| -------------------------------- | ------------------------------------------------- | ------------------------------- |
| **Time-based** (default: 7 days) | Messages expire after set time                    | Event data                      |
| **Size-based**                   | Messages expire when partition reaches size limit | Bounded storage                 |
| **Log compaction**               | Retains latest value per key                      | State snapshots, event sourcing |
| **Hybrid** (delete + compact)    | Both policies active                              | CDC with fast-moving data       |

---

## PART III: USE CASES & PATTERNS

### 12. Kafka Use Cases

| Use Case                           | Description                                                         |
| ---------------------------------- | ------------------------------------------------------------------- |
| **Publish-subscribe**              | Loosely-coupled microservices communication                         |
| **Log aggregation**                | Collect logs from distributed sources; Kafka acts as durable buffer |
| **Log shipping**                   | Real-time replication of journal entries for read replicas          |
| **SEDA pipelines**                 | Staged event-driven processing with independent scaling per stage   |
| **Complex Event Processing (CEP)** | Pattern detection across event streams (fraud, trading, etc.)       |
| **Event-sourced CQRS**             | Separating read/write models with Kafka as the event store          |
| **Activity tracking**              | Original Kafka use case — user actions, page views                  |
| **Metrics & monitoring**           | Centralised operational data feeds                                  |
| **Commit log**                     | Database CDC and changelog streams                                  |

### 13. Architectural Patterns

**Event-oriented broadcast:** Producer "is king" — owns the topic lifecycle, schema, and partitioning. Consumers only decide whether to subscribe.

**Peer-to-peer messaging:** Consumer plays the role of service provider; owns the topic. Used for command-response patterns.

**Topic conditioning (SEDA):** Intermediate processing stages reconcile producer-oriented data with consumer-specific requirements. Avoids coupling while allowing diverse consumer needs.

**Key-centric partitioning:** Events keyed by stable entity identifiers (e.g., match ID, customer ID) ensure causal ordering within each entity while allowing parallel processing across entities.

---

## PART IV: DESIGN BEST PRACTICES

### 14. Topic Design

**Partition count:**

- Start over-provisioned (order of magnitude more than expected) — Kafka cannot non-destructively reduce partitions
- Confluent recommends: `100 × b × r` partitions/broker max (b=brokers, r=replication factor)
- Practical ceiling: ~4,000 partitions/broker, ~200,000 partitions/cluster

> **Warning:** Increasing partition count breaks key-based ordering guarantees — the hash changes when `numPartitions` changes.

**Topic ownership:**

- In broadcast mode, the **producer** owns the topic (lifecycle, config, schema, sizing)
- In peer-to-peer mode, the **consumer** (service provider) owns the topic

### 15. Producer Best Practices

| Practice                        | Detail                                                         |
| ------------------------------- | -------------------------------------------------------------- |
| **Always enable idempotence**   | `enable.idempotence=true` prevents duplicates from retries     |
| **Use `acks=all`**              | Ensures writes are replicated to all ISR before acknowledgment |
| **Set `delivery.timeout.ms`**   | Acts as overarching budget for send + retries                  |
| **Enable compression**          | `lz4` or `zstd` for most workloads                             |
| **Use constants**               | `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG` prevents typos       |
| **Layer your code**             | Separate business logic from Kafka concerns                    |
| **Use type-safe configuration** | Validate property names before client initialisation           |

### 16. Consumer Best Practices

| Practice                             | Detail                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------ |
| **Manual offset commits**            | Disable `enable.auto.commit`; commit after processing, not before        |
| **Set `auto.offset.reset=earliest`** | Default `latest` can skip records (at-most-once behaviour)               |
| **Handle rebalances properly**       | Commit offsets in `onPartitionsRevoked()` callback                       |
| **Monitor consumer lag**             | Use Burrow, Kafdrop, or custom metrics                                   |
| **Design for idempotence**           | Processing the same record twice should have no net effect               |
| **Avoid blocking in poll loop**      | Stay within `max.poll.interval.ms` (default 5 min) to prevent revocation |

### 17. Configuration Safety Checklist

Kafka's defaults favour **performance over safety** — explicitly configure for safety:

| Setting                          | Safe Value      | Default (Unsafe) |
| -------------------------------- | --------------- | ---------------- |
| `acks`                           | `all`           | `1`              |
| `enable.idempotence`             | `true`          | `false`          |
| `min.insync.replicas`            | `2` (with RF=3) | `1`              |
| `unclean.leader.election.enable` | `false`         | `false` ✓        |
| `auto.offset.reset`              | `earliest`      | `latest`         |
| `enable.auto.commit`             | `false`         | `true`           |
| `replication.factor`             | `3`             | `1`              |

### 18. Security Best Practices

**Target state (hardened cluster):**

1. Network-level traffic policy (firewall) — segment ZooKeeper, broker, and client networks
2. TLS/SSL encryption for all data in transit
3. Client authentication (mTLS or SASL/SCRAM)
4. Authorization via ACLs (default-deny model)

**Authentication methods compared:**

| Method                     | Strength                         | Complexity | Use Case                                |
| -------------------------- | -------------------------------- | ---------- | --------------------------------------- |
| **mTLS**                   | High (certificate-based)         | High       | Machine-to-machine, managed Kafka       |
| **SASL/SCRAM**             | High (salted challenge-response) | Medium     | Username/password with Defence in Depth |
| **SASL/PLAIN**             | Low (cleartext, needs TLS)       | Low        | Legacy compatibility                    |
| **SASL/GSSAPI (Kerberos)** | High                             | Very high  | Corporate AD environments               |
| **Delegation tokens**      | High (time-bounded)              | Medium     | Ephemeral worker nodes                  |

**ACL quick reference:**

| Scenario             | Required ACLs                                                                         |
| -------------------- | ------------------------------------------------------------------------------------- |
| Publish to a topic   | `Write` (+ `IdempotentWrite` on Cluster if idempotence enabled) + `Describe` on topic |
| Consume from a topic | `Read` + `Describe` on topic + `Read` on consumer group                               |
| Create/delete topics | `ClusterAction` on Cluster                                                            |
| Admin operations     | `AlterConfigs`, `DescribeConfigs` on relevant resources                               |

### 19. Batching and Compression

**Why batch:** Network is typically the bottleneck, not disk. Batching amortises round-trip overhead.

**Key properties:**

- `batch.size` (default: 16 KB) — caps batch in bytes
- `linger.ms` (default: 0) — artificial delay to accumulate more records under low load

**Compression recommendations:**

- Always enable compression for text-based encodings (JSON, XML)
- **LZ4:** Low overhead, good for fast networks
- **ZStandard:** Best balance of ratio and speed (Kafka 2.1+); preferred when network is the bottleneck
- **Gzip:** Highest compression ratio, higher CPU cost
- Disable compression when using end-to-end encryption (entropy already maximal)

### 20. Quotas

**Two types:**

1. **Network bandwidth quotas** (`producer_byte_rate`, `consumer_byte_rate`) — limit throughput in B/s
2. **Request rate quotas** (`request_percentage`) — limit CPU utilisation as % of thread

**Enforcement:** Sliding window algorithm with `quota.window.num` (default: 11) and `quota.window.size.seconds` (default: 1).

**Delay formula:** When observed utilisation $U$ exceeds quota $Q$:
$$D = T \cdot \frac{U - Q}{Q}$$
where $T = \text{window\_num} \times \text{window\_size}$.

**Gotcha:** Quotas are per-broker — connecting to $N$ brokers gives $N \times$ the effective quota.

### 21. Transactions

**Problem:** In consume-transform-produce, the stage may publish the output but crash before committing input offsets → duplicate records on recovery.

**Solution:** Atomic writes across multiple partitions and topics via a **transaction coordinator**.

**Key API methods:**

- `initTransactions()` — initialises PID with epoch (fences zombies)
- `beginTransaction()` — starts transaction scope
- `sendOffsetsToTransaction()` — atomically commits consumer offsets with produced records
- `commitTransaction()` / `abortTransaction()` — finalises or discards

**Transactional ID strategy:** Use **pinned producers** — one producer per input partition, with `transactional.id` derived from `topic-partition`.

**Limitations:**

1. Kafka resources only (no XA/JTA)
2. Cannot span producers or clusters
3. May be partially observed across partitions
4. Does not make external side-effects idempotent — application must handle that

**Practical advice:** For most apps, `enable.idempotence=true` provides the best cost-benefit ratio. Kafka Streams transparently handles transactions for exactly-once processing.

### 22. Delivery Semantics

| Guarantee         | How to achieve                                                           |
| ----------------- | ------------------------------------------------------------------------ |
| **At-most-once**  | Commit offsets before processing; disable producer retries               |
| **At-least-once** | Commit offsets after processing; enable producer retries (Kafka default) |
| **Exactly-once**  | At-least-once delivery + consumer idempotence (or transactions)          |

> **Key insight:** Exactly-once semantics are **not possible at the middleware layer alone** — they require tight collaboration with the application. A messaging platform cannot offer exactly-once guarantees on its own; the consumer must be idempotent.

### 23. Serialization and Layering

**Best practice:** Separate Kafka-specific messaging code from business logic using an abstraction layer:

```
Business Logic ↔ EventSender/EventReceiver (interface) ↔ KafkaProducer/KafkaConsumer
```

Benefits:

- Enforces invariants (e.g., key = entity ID)
- Simplifies mocking and testing
- Encapsulates serialization/deserialization
- Enables pipelining (separate deserialization from processing)

**Custom vs piggyback serializers:**

- Custom serializers provide type safety at the Kafka client level
- Piggybacking (using `StringSerializer` + application-level marshalling) is simpler for prototyping
- For production: custom deserializers with error handling for corrupt records (dead-letter topics)

---

## PART V: OPERATIONAL GUIDANCE

### 24. Monitoring

| Metric                          | What to Watch                                               |
| ------------------------------- | ----------------------------------------------------------- |
| **Under-replicated partitions** | > 0 = investigate immediately                               |
| **Consumer lag**                | Track per group; rising lag indicates processing bottleneck |
| **Request handler idle ratio**  | Low = thread saturation                                     |
| **Broker CPU**                  | Keep < 60% for headroom                                     |
| **Disk usage**                  | Alarm at 85%                                                |
| **Heap after GC**               | Alarm above 60%                                             |

### 25. Capacity Planning

- **Disk:** `daily_ingest × retention_days × replication_factor × 1.1` (10% overhead)
- **Partitions/broker:** Max ~4,000 (AWS MSK guidance); Confluent: `100 × b × r`
- **CPU:** Keep user + system < 60%
- **Network:** 10 GbE minimum; replication multiplies ingress

### 26. Common Pitfalls (Gotchas)

| #   | Pitfall                                                  | Solution                                                                      |
| --- | -------------------------------------------------------- | ----------------------------------------------------------------------------- |
| 1   | Assuming Kafka is "durable by default"                   | Explicitly set `acks=all`, `enable.idempotence=true`, `min.insync.replicas=2` |
| 2   | Using default `auto.offset.reset=latest`                 | Set to `earliest` for at-least-once semantics                                 |
| 3   | Resizing a topic with ordered keys                       | Cannot! Hashing changes; create new topic and migrate                         |
| 4   | Not validating client property names                     | Use static constants (`ProducerConfig.*`) and type-safe config classes        |
| 5   | Assuming ZooKeeper connection string = bootstrap servers | They are completely different!                                                |
| 6   | Running without authentication/ACLs                      | Kafka is not secure by default; follows default-deny model                    |
| 7   | Not handling rebalances properly                         | Commit offsets in `onPartitionsRevoked()`                                     |
| 8   | Using transactions without understanding limitations     | They don't make external systems idempotent                                   |
| 9   | Over-relying on enable.auto.commit                       | Can lead to at-most-once delivery; use manual commits                         |
| 10  | Not monitoring consumer lag                              | Silent data loss or processing delays                                         |

---

_This summary synthesises key concepts from "Effective Kafka" by Emil Koutanov (Leanpub, 2021). For detailed worked examples with Java code, refer to the full book and the accompanying source code at github.com/ekoutanov/effectivekafka._
