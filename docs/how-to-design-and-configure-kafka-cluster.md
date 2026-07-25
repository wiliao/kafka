# How to Design and Configure a Kafka Cluster

Sources: _Kafka: The Definitive Guide_, 2nd Edition (O'Reilly, 2021) — see
[Kafka_The_Definitive_Guide_2nd_Edition.md](Kafka_The_Definitive_Guide_2nd_Edition.md) and
[kafka-core-concepts.md](kafka-core-concepts.md) — plus current industry best practices
(Apache Kafka 4.3 documentation, AWS MSK best practices, Confluent recommendations).

---

## 1. Start with Requirements

Before touching any configuration, quantify four things:

| Requirement  | Questions to Answer                                                         |
| ------------ | --------------------------------------------------------------------------- |
| Throughput   | Peak and average MB/s in and out? Messages/second and average message size? |
| Retention    | How long must data be kept? (drives disk capacity)                          |
| Availability | How many broker/AZ failures must the cluster survive? RPO/RTO targets?      |
| Latency      | Produce/consume latency budget? (drives disk and network choices)           |

**Disk capacity rule of thumb** (from the book): daily ingest × retention days, plus headroom.

> Example: 1 TB/day × 7 days retention = 7 TB minimum across the cluster — then multiply by
> the replication factor and add ~10% for overhead.

---

## 2. Architecture Decisions

### 2.1 KRaft vs. ZooKeeper

The book (Kafka 2.7/2.8 era): ZooKeeper stores broker metadata; run an ensemble of an odd
number of servers (3, 5, or 7). KRaft was a preview in 2.8, early-access in 3.0–3.2.

**Today:** KRaft is the only mode — ZooKeeper support was completely removed in Apache Kafka 4.0.
New clusters should always use KRaft. KRaft became production-ready in 3.3 (not 3.0 — earlier
versions were early-access only).

**KRaft controller deployment:**

- Use **3 controllers** for production (tolerates 1 failure); **5** for very large clusters.
  Never an even number.
- **Combined mode** (`process.roles=broker,controller`) — acceptable for development and small
  clusters (3–5 nodes).
- **Dedicated controllers** (`process.roles=controller` on separate nodes) — recommended for
  production and large clusters, isolating metadata traffic from data traffic.

<!-- NEW: Dynamic controller membership -->

**Dynamic controller membership (Kafka 4.1+):** Controllers can be added to or removed from a
running cluster at runtime using `kafka-metadata-quorum.sh add-controller` /
`remove-controller`, eliminating the need for a full cluster restart when scaling the
controller quorum. See the
[KRaft operations page](https://kafka.apache.org/43/operations/kraft/) for details.

### 2.2 How Many Brokers?

Determined by (per the book):

- **Disk capacity** — total retention requirement ÷ usable disk per broker
- **Replica capacity per broker** — stay within partition-count limits (see §3.2)
- **CPU capacity** — compression and request handling
- **Network capacity** — replication multiplies ingress traffic (RF=3 roughly doubles write
  traffic)

Keep broker CPU utilization (user + system) under **60%** so the cluster retains headroom for
rolling upgrades, patching, and broker failures
([AWS MSK guidance](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html)).

### 2.3 Single Cluster vs. Multiple Clusters

Use multiple clusters for: regional aggregation, HA/disaster recovery, regulatory compliance,
cloud migration, and edge aggregation. Common topologies (book Ch10):

- **Hub-and-spoke** — edge clusters feed a central aggregate cluster
- **Active-active** — all clusters serve reads/writes (beware conflict handling)
- **Active-standby** — primary + DR backup
- **Stretch cluster** — one cluster spanning datacenters (only with excellent inter-DC
  networking)

Mirror data with **MirrorMaker 2** (built on Kafka Connect; supports offset translation, topic
config and ACL migration, automatic new-topic detection).

### 2.4 Rack / Availability-Zone Awareness

- Spread brokers across at least **3 AZs** (or racks).
- Set `broker.rack` (or the cloud equivalent) so Kafka places replicas of each partition in
  different failure domains.
- Ensure client connection strings include at least one broker per AZ for failover.

---

## 3. Hardware Selection

### 3.1 What Matters (book Ch2)

| Resource        | Guidance                                                                                                                                        |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Disk throughput | The biggest latency factor. Prefer SSD/NVMe; faster writes = lower produce latency                                                              |
| Disk capacity   | Driven by retention (see §1)                                                                                                                    |
| Memory          | Kafka uses a modest heap (~5 GB); the rest becomes **OS page cache**, which serves most consumer reads — more RAM = better consumer performance |
| Network         | 10 GbE minimum; replication and consumers multiply egress                                                                                       |
| CPU             | Less critical, but matters at scale when compression is enabled                                                                                 |

### 3.2 Cloud Instances

Book (2021): Azure Standard D16s v3 (small) / D64s v4 (large) with managed disks; AWS m4 or r3
families.

**AWS MSK today** — recommended partitions per broker (leader + follower replicas):

| Broker size                                         | Recommended partitions/broker |
| --------------------------------------------------- | ----------------------------- |
| kafka.t3.small                                      | 300                           |
| kafka.m5.large / m5.xlarge, m7g.large / m7g.xlarge  | 1,000                         |
| kafka.m5.2xlarge, m7g.2xlarge                       | 2,000                         |
| kafka.m5.4xlarge and larger, m7g.4xlarge and larger | 4,000                         |

**General ceiling:** ~4,000 partitions per broker and ~200,000 partitions per cluster
(Apache Kafka/Confluent guidance). High partition counts also inflate metrics and
offset-tracking overhead — delete unused consumer groups.

### 3.3 Disk Layout: JBOD vs. RAID

- **JBOD** (multiple `log.dirs` paths, one per disk) is the common recommendation: more
  capacity and throughput; Kafka's own replication provides durability, so RAID redundancy is
  largely wasted.
- **RAID 10** simplifies failure handling (a disk loss doesn't take a log dir offline) but
  costs capacity and some throughput.
- **Caveat:** Tiered Storage requires a single mount point and does not support JBOD
  (Confluent Platform note).

**Tiered Storage** (Kafka 3.6+, GA in 4.x): offloads older log segments to cheap remote
storage (S3, GCS, etc.), decoupling retention from local disk capacity. If you plan to use it,
factor a single-mount layout into your disk design. See
[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
for details.

<!-- NEW: Kafka 4.3 tiered storage updates -->

**Kafka 4.3 tiered storage updates:**

| Config                                       | Purpose                                                                                                                                                                  |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `remote.log.metadata.topic.min.isr`          | Explicitly sets min ISR for the internal `__remote_log_metadata` topic (default: 2). Fixes a correctness issue where the topic could become under-replicated. (KIP-1235) |
| `follower.fetch.last.tiered.offset.enable`   | When `true`, new followers bootstrap from the last tiered offset instead of the beginning of the local log — significantly faster scaling. Default: `false`. (KIP-1023)  |
| `remote.log.metadata.admin.<kafka.property>` | Prefix for configuring the admin client used by `RemoteLogMetadataManager`. (KIP-1208)                                                                                   |

> **Deprecation note:** `remote.log.manager.thread.pool.size` is deprecated since Kafka 4.2;
> use `remote.log.manager.follower.thread.pool.size` instead.

---

## 4. OS and JVM Tuning

### 4.1 Operating System (book Ch2)

- **OS:** Linux (Kafka is optimized for it)
- **Virtual memory:** `vm.swappiness=1`; tune `vm.dirty_background_ratio` and `vm.dirty_ratio`
- **Filesystem:** XFS with the `noatime` mount option
- **Networking:** increase socket buffer sizes; enable TCP window scaling

### 4.2 JVM and Garbage Collection

- **Heap:** keep modest — 5–6 GB is typical; Kafka's performance comes from page cache, not
  heap. Alarm if heap-used-after-GC exceeds 60%.
- **GC:** G1GC with `MaxGCPauseMillis=20` and `InitiatingHeapOccupancyPercent=35` (book
  recommendation, still the standard starting point).

<!-- CHANGED: Updated Java version support for Kafka 4.3 -->

- **Java version:** Kafka 4.x requires **Java 17+** for brokers and controllers.
  - **Kafka 4.3** fully supports Java **17, 21, and 25** for all modules (brokers, controllers,
    clients, streams).
  - **Java 11** is supported only for a subset of modules (clients, streams) — not for brokers
    or controllers.
  - **Java 8** was completely removed in Kafka 4.0.
  - Check your Kafka version's
    [support matrix](https://kafka.apache.org/43/operations/java-version/) before upgrading.

---

## 5. Broker Configuration

### 5.1 General Parameters

```properties
# Identity — unique integer per broker (KRaft uses node.id; broker.id is for legacy ZK mode)
node.id=1

# Client endpoint
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker1.example.com:9092

# Storage — one path per disk for JBOD
log.dirs=/data1/kafka-logs,/data2/kafka-logs

# Faster log recovery after unclean shutdown (set to # of CPU cores)
num.recovery.threads.per.data.dir=16

# Disable auto topic creation in production
auto.create.topics.enable=false

# Rack/AZ awareness
broker.rack=us-east-1a
```

### 5.2 Topic Defaults

```properties
num.partitions=8                    # default for new topics (book default: 1 — raise deliberately)
                                    # ⚠ Partition count can only be increased, never decreased
                                    #   (without recreating the topic)
default.replication.factor=3
log.retention.hours=168             # 1 week (book default)
log.retention.bytes=-1              # or cap per-partition size
log.segment.bytes=1073741824        # 1 GB (book default)
message.max.bytes=1048588           # ~1 MB (broker default)  <!-- CHANGED: was 1000000 -->
min.insync.replicas=2
```

### 5.3 Thread Tuning (larger brokers)

Scale request-handling threads with CPU cores (AWS MSK guidance):

| Instance size                 | `num.io.threads` | `num.network.threads` |
| ----------------------------- | ---------------- | --------------------- |
| 16 cores (m5.4xl / m7g.4xl)   | 16               | 8                     |
| 32 cores (m5.8xl / m7g.8xl)   | 32               | 16                    |
| 48 cores (m5.12xl / m7g.12xl) | 48               | 24                    |
| 64 cores (m5.16xl / m7g.16xl) | 64               | 32                    |

Increase `num.io.threads` **before** `num.network.threads` to avoid queue congestion.

### 5.4 KRaft-Specific Configuration

<!-- CHANGED: Rewritten to lead with the current recommended config (controller.quorum.bootstrap.servers) -->

**Recommended for new clusters (Kafka 4.1+, kraft.version=1):**

```properties
process.roles=broker                # or controller, or broker,controller (combined)
node.id=1

# Dynamic quorum — no node IDs needed, just host:port list
controller.quorum.bootstrap.servers=ctrl1:9093,ctrl2:9093,ctrl3:9093

controller.listener.names=CONTROLLER
listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT
listeners=BROKER://0.0.0.0:9092
```

> **Deprecation note:** `controller.quorum.voters` (the static-quorum syntax
> `1@ctrl1:9093,2@ctrl2:9093,3@ctrl3:9093`) is deprecated since **Kafka 4.1**
> (kraft.version=1). It is still accepted for backward compatibility with existing static
> quorums, but new clusters should use `controller.quorum.bootstrap.servers`. The dynamic
> quorum feature was first introduced in Kafka 3.9 (KIP-853).
>
> <!-- CHANGED: was "deprecated since Kafka 3.4+" -->

<!-- NEW: KRaft fetch size controls -->

**KRaft metadata fetch controls (Kafka 4.3, KIP-1219):**

For large clusters or combined-mode deployments with high metadata throughput, cap the data
retrieved by controller fetch requests:

```properties
controller.quorum.fetch.max.bytes=1048576          # default 1 MB; increase for large clusters
controller.quorum.fetch.snapshot.max.bytes=10485760 # default 10 MB
```

**New cluster provisioning workflow (Kafka 4.3):**

<!-- CHANGED: Expanded from single sentence to full workflow -->

1. **Generate a cluster ID:**

   ```bash
   KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
   ```

2. **Format storage.** Choose one of the following approaches:
   - **Standalone bootstrap** (preferred for new clusters — start one controller, then
     dynamically add the rest):

     ```bash
     bin/kafka-storage.sh format \
         --cluster-id $KAFKA_CLUSTER_ID \
         --standalone \
         --config config/controller.properties
     ```

     After the first controller is running, add remaining controllers dynamically:

     ```bash
     bin/kafka-metadata-quorum.sh add-controller \
         --bootstrap-server ctrl1:9093 \
         --config config/controller-2.properties
     ```

   - **Multi-controller bootstrap** (all controllers formatted at once):

     ```bash
     bin/kafka-storage.sh format \
         --cluster-id $KAFKA_CLUSTER_ID \
         --initial-controllers "0@ctrl-0:9093:${UUID0},1@ctrl-1:9093:${UUID1},2@ctrl-2:9093:${UUID2}" \
         --config config/controller.properties
     ```

   - **Joining an existing cluster** (new brokers or controllers added later):
     ```bash
     bin/kafka-storage.sh format \
         --cluster-id $KAFKA_CLUSTER_ID \
         --config config/server.properties \
         --no-initial-controllers
     ```

3. **Start the brokers/controllers.**

---

## 6. Reliability Configuration (book Ch7)

The durability baseline — apply at broker/topic level and client level:

| Setting                          | Value             | Why                                                                                                                                                          |
| -------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `replication.factor`             | 3                 | Survive broker loss; RF=1 risks offline partitions, RF=2 risks data loss during rolling ops                                                                  |
| `min.insync.replicas`            | RF − 1 (=2)       | Writes succeed with one replica offline; minISR = RF would block writes during maintenance                                                                   |
| `unclean.leader.election.enable` | `false` (default) | Prevents out-of-sync replicas becoming leader (data loss)                                                                                                    |
| Producer `acks`                  | `all`             | Wait for all in-sync replicas                                                                                                                                |
| Producer `enable.idempotence`    | `true`            | No duplicates from retries (requires `acks=all`, `retries>0`; since Kafka 3.0+ the client enforces `max.in.flight.requests.per.connection<=5` automatically) |
| Consumer `enable.auto.commit`    | `false`           | Commit offsets only after messages are fully processed                                                                                                       |

<!-- NEW: Eligible Leader Replicas note -->

> **Eligible Leader Replicas (ELR) — Kafka 4.x:** When the ELR feature is enabled, the
> semantics of `min.insync.replicas` change. ELR allows a broader set of replicas to be
> eligible for leader election, improving availability during extended outages. Review the
> [Kafka 4.3 broker config docs](https://kafka.apache.org/43/configuration/#brokerconfigs_min.insync.replicas)
> for the updated semantics before enabling ELR in production.

Also: rely on replication, not fsync, for durability — Kafka flushes via the OS page cache;
tune `flush.*` settings with care.

---

## 7. Security Baseline (book Ch11)

- **Encryption in transit:** TLS between clients and brokers, and between brokers (enable
  everywhere in production)
- **Authentication:** SSL client certificates or SASL (SCRAM-SHA-512 preferred over PLAIN;
  OAUTHBEARER for OAuth 2.0; GSSAPI for Kerberos)
- **Authorization:** `AclAuthorizer` with least-privilege ACLs
- **ZooKeeper** (legacy clusters only): secure with SASL/SSL and ACLs
- **Auditing:** broker logs capture authentication success/failure

<!-- NEW: OAuth client assertions -->

> **Kafka 4.3:** OAUTHBEARER now supports **OAuth client assertions** (KIP-1258) for
> machine-to-machine authentication without client secrets. See the
> [security configuration docs](https://kafka.apache.org/43/configuration/#security) for
> setup details.

---

## 8. Monitoring and Operations (book Ch13 + industry)

### 8.1 Metrics to Watch

| Metric                                             | Threshold / Action                                                                                                                                                                                                                        |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Under-replicated partitions                        | > 0 = investigate immediately                                                                                                                                                                                                             |
| Active controller count                            | Must be exactly 1 per cluster (in KRaft, `ActiveControllerCount` reports 1 only on the current leader; other controllers report 0)                                                                                                        |
| Offline partitions                                 | > 0 = data unavailable                                                                                                                                                                                                                    |
| Broker CPU (user + system)                         | Keep < 60%; scale up/out above that                                                                                                                                                                                                       |
| Disk usage (`KafkaDataLogsDiskUsed` or equivalent) | Alarm at 85% — expand storage, shorten retention, or delete unused topics                                                                                                                                                                 |
| Heap memory after GC                               | Alarm above 60%                                                                                                                                                                                                                           |
| Consumer lag                                       | Track per group (Burrow or equivalent)                                                                                                                                                                                                    |
| Request handler idle ratio                         | Low idle = thread saturation                                                                                                                                                                                                              |
| **Partition size % of max retention**              | <!-- NEW: KIP-1257 --> New in Kafka 4.3: JMX metrics expose what percentage of max retention each partition uses. Use these to proactively identify partitions approaching retention limits before they cause data loss or disk pressure. |

Define SLIs/SLOs (e.g., produce latency, availability) and alert on burn rate rather than raw
metric spikes.

### 8.2 Operational Practices

- **Rebalancing:** use Cruise Control for continuous load distribution; reassign partitions in
  small batches (≤ ~10 partitions per `kafka-reassign-partitions` call as a conservative
  starting point — larger batches are feasible with monitoring) and avoid reassignment when
  CPU > 70% (replication adds load).

<!-- NEW: Broker cordoning -->

- **Broker/log directory cordoning (Kafka 4.3, KIP-1066):** Use the `cordoned.log.dirs`
  broker config to mark specific log directories as off-limits for new partition placement.
  This is the modern mechanism for planned decommissioning — cordon the directories, let
  Cruise Control (or manual reassignment) migrate partitions away, then remove the broker.
  This replaces much of the manual coordination previously needed for safe decommissioning.

- **Retention management:** topic-level `retention.ms` / `retention.bytes` override cluster
  defaults — use them to free disk selectively.
- **Scaling:** prefer scaling up (bigger brokers) for CPU headroom, or out (more brokers +
  new partitions) for granular growth; verify new partitions land on new brokers with
  `kafka-topics.sh --describe`.
- **Rolling changes:** one broker at a time; with RF=3 and minISR=2 the cluster stays fully
  available.
- **Log recovery:** after an unclean shutdown, recovery uses one thread per log dir by
  default — set `num.recovery.threads.per.data.dir` to the core count to avoid hours-long
  restarts.
- **Prometheus scraping:** use a ≥ 60s scrape interval to avoid CPU overhead.

---

## 9. Client-Side Considerations

Cluster design fails if clients can't tolerate broker loss:

- `bootstrap.servers` should list multiple brokers across AZs (not a single broker or LB
  pointing at one).
- **Producers:** `acks=all`, idempotence enabled, `delivery.timeout.ms` tuned to the latency
  budget.
- **Consumers:** manual offset commits after processing; use static group membership
  (`group.instance.id`) to reduce rebalances during rolling restarts.

<!-- NEW: Classic rebalance protocol deprecation -->

> **Rebalance protocol migration (Kafka 4.3, KIP-1274):** The `classic` consumer rebalance
> protocol is deprecated (Phase 1). Consumers using `partition.assignment.strategy` with the
> classic protocol will now emit a warning log. Plan migration to the **cooperative**
> rebalance protocol (`CooperativeStickyAssignor`) before Kafka 5.0, where the classic
> protocol is expected to be removed.
>
> Additionally, the broker config `group.coordinator.rebalance.protocols` is deprecated in
> 4.3 (KIP-1237) and will be removed in 5.0.

- Run performance tests to verify client configs meet objectives before production.

---

## 10. Reference Checklist

### Design

- [ ] Throughput, retention, availability, latency requirements documented
- [ ] KRaft mode with 3 dedicated controllers (production)
- [ ] Brokers across 3 AZs with `broker.rack` set
- [ ] Partition plan: ≤ 4,000 replicas/broker, ≤ 200K/cluster

### Configuration

- [ ] `default.replication.factor=3`, `min.insync.replicas=2`
- [ ] `unclean.leader.election.enable=false`
- [ ] `auto.create.topics.enable=false`
- [ ] Retention set per topic; `log.dirs` on dedicated disks (JBOD) or RAID 10
- [ ] `vm.swappiness=1`, XFS + `noatime`, G1GC with 5–6 GB heap
- [ ] TLS + SASL/SCRAM (or mTLS) + ACLs enabled
- [ ] `controller.quorum.bootstrap.servers` configured (not deprecated `controller.quorum.voters`) <!-- NEW -->
- [ ] Java 17+ (17/21/25 for Kafka 4.3) confirmed for all nodes <!-- NEW -->

### Operations

- [ ] Alerts: under-replicated partitions, offline partitions, CPU > 60%, disk > 85%,
      heap-after-GC > 60%
- [ ] Cruise Control (or equivalent) for rebalancing
- [ ] Consumer lag monitoring in place
- [ ] Rolling upgrade/runbook tested
- [ ] Broker cordoning workflow documented for decommissioning (Kafka 4.3+) <!-- NEW -->
- [ ] Consumer rebalance protocol migration plan (classic → cooperative) documented <!-- NEW -->

---

Book-derived facts reference _Kafka: The Definitive Guide_, 2nd Edition (Shapira, Palino,
Sivaram, Petty). Industry guidance: Apache Kafka 4.3 documentation (KRaft, configuration),
AWS MSK Best Practices for Standard brokers, Confluent Platform deployment docs.

---

## Appendix A: Review Fix Summary

The following corrections were applied after technical reviews to improve accuracy and recency:

| #   | Fix                                                                                                                                                                                                                                                                                             | Section          |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 1   | Changed `broker.id=1` → `node.id=1`; updated comment to clarify KRaft uses `node.id`, `broker.id` is for legacy ZK mode                                                                                                                                                                         | §5.1             |
| 2   | Added missing `listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT` so the KRaft config block is self-contained                                                                                                                                                                | §5.4             |
| 3   | **Corrected** deprecation note: `controller.quorum.voters` is deprecated since **Kafka 4.1** (kraft.version=1), not Kafka 3.4+. Dynamic quorum (`controller.quorum.bootstrap.servers`) was first introduced in Kafka 3.9 (KIP-853). Updated config example to lead with the recommended syntax. | §5.4             |
| 4   | Added warning that partition count can only be **increased**, never decreased (without recreating the topic)                                                                                                                                                                                    | §5.2             |
| 5   | Updated idempotence note: since Kafka 3.0+ the client enforces `max.in.flight.requests.per.connection<=5` automatically — no manual tuning needed                                                                                                                                               | §6               |
| 6   | Corrected KRaft timeline: was a preview in 2.8, early-access in 3.0–3.2, production-ready in **3.3** (not 3.0)                                                                                                                                                                                  | §2.1             |
| 7   | Clarified `ActiveControllerCount` behavior in KRaft: reports 1 only on the current leader; other controllers report 0                                                                                                                                                                           | §8.1             |
| 8   | Added Tiered Storage callout block with KIP-405 link (offloads older segments to S3/GCS, requires single-mount layout)                                                                                                                                                                          | §3.3             |
| 9   | Added Java 21 support note (Kafka 3.7+), AWS MSK guidance hyperlink, and nuance on partition reassignment batch sizes                                                                                                                                                                           | §4.2, §2.2, §8.2 |
| 10  | **Fixed** `message.max.bytes` default from `1000000` to `1048588` (actual broker default per Kafka 4.3 docs)                                                                                                                                                                                    | §5.2             |
| 11  | **Updated** Java version support: Kafka 4.3 supports Java 17, 21, and 25; Java 11 is clients/streams only; Java 8 removed in Kafka 4.0                                                                                                                                                          | §4.2             |
| 12  | **Expanded** KRaft provisioning workflow: added `--standalone`, `--initial-controllers`, and `--no-initial-controllers` flags; documented dynamic controller add/remove via `kafka-metadata-quorum.sh`                                                                                          | §5.4, §2.1       |
| 13  | **Added** Kafka 4.3 tiered storage configs: `remote.log.metadata.topic.min.isr` (KIP-1235), `follower.fetch.last.tiered.offset.enable` (KIP-1023), `remote.log.metadata.admin.*` prefix (KIP-1208); noted `remote.log.manager.thread.pool.size` deprecation                                     | §3.3             |
| 14  | **Added** KRaft fetch size controls: `controller.quorum.fetch.max.bytes`, `controller.quorum.fetch.snapshot.max.bytes` (KIP-1219)                                                                                                                                                               | §5.4             |
| 15  | **Added** broker/log directory cordoning via `cordoned.log.dirs` (KIP-1066) as the modern decommissioning workflow                                                                                                                                                                              | §8.2             |
| 16  | **Added** partition size percentage metrics (KIP-1257) to monitoring table                                                                                                                                                                                                                      | §8.1             |
| 17  | **Added** Eligible Leader Replicas (ELR) note on changed `min.insync.replicas` semantics                                                                                                                                                                                                        | §6               |
| 18  | **Added** classic rebalance protocol deprecation (Phase 1, KIP-1274) and `group.coordinator.rebalance.protocols` deprecation (KIP-1237); recommended cooperative protocol migration                                                                                                             | §9               |
| 19  | **Added** OAUTHBEARER client assertions note (KIP-1258)                                                                                                                                                                                                                                         | §7               |

---

## Appendix B: Minor Gaps (Cross-Reference with Configuration Docs)

The following gaps were identified by cross-referencing this document against the
`docs/configuration/*.md` reference files and the Kafka 4.3 online documentation. These are
not errors — they reflect the design-oriented scope of this guide — but are noted for future
updates.

| #   | Gap                                                       | Details                                                                                                                                                                                                                                                                                                                     | Related Section                    |
| --- | --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| 1   | Dynamic config updates                                    | `docs/configuration/broker-configs.md` extensively covers dynamic updates (per-broker, cluster-wide, SSL keystores/truststores, listener add/remove). This guide focuses on initial design, not runtime operations.                                                                                                         | §5 (Broker Configuration)          |
| 2   | Configuration Providers                                   | `docs/configuration/configuration-providers.md` documents `FileConfigProvider`, `EnvVarConfigProvider`, and `DirectoryConfigProvider` for loading secrets and config from external sources. Not covered here.                                                                                                               | §7 (Security)                      |
| 3   | OAUTHBEARER system property                               | `docs/configuration/system-properties.md` documents `org.apache.kafka.sasl.oauthbearer.allowed.files` (since Kafka 4.1.0) for restricting which files the SASL OAUTHBEARER plugin can read. Not referenced here.                                                                                                            | §7 (Security)                      |
| 4   | Group configs                                             | `docs/configuration/group-configs.md` provides configuration for consumer, share, and streams groups. This guide references consumer groups in passing but does not detail group-level settings.                                                                                                                            | §6 (Reliability), §9 (Client-Side) |
| 5   | `message.max.bytes` vs `max.message.bytes`                | This guide lists `message.max.bytes` (broker-level default) under topic defaults in §5.2. The topic config reference uses `max.message.bytes` (the per-topic override name). These are different properties and both names are technically correct in their respective contexts, but the juxtaposition may confuse readers. | §5.2 (Topic Defaults)              |
| 6   | Share groups (Kafka 4.x)                                  | Kafka 4.x introduces **share groups** as a new consumer group type with queue-like semantics. Share group operational tuning configs were added in 4.3 (KIP-1240, KIP-1263). Not covered in this guide.                                                                                                                     | §9 (Client-Side)                   |
| 7   | `num.partitions` / `default.replication.factor` alignment | KIP-1211 (Kafka 4.3) aligns the behavior of `num.partitions` and `default.replication.factor` for topic creation, fixing prior inconsistencies. No config change needed, but operators upgrading to 4.3 should be aware of the behavioral change.                                                                           | §5.2 (Topic Defaults)              |
| 8   | ELR detailed configuration                                | The Eligible Leader Replicas feature (noted in §6) has its own set of broker configs (`eligible.leader.replicas.*`). A full treatment is outside this guide's scope but should be reviewed before enabling in production.                                                                                                   | §6 (Reliability)                   |

## Change Summary

| Category                     | Count | Key Items                                                                                                                                                                                                      |
| ---------------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Errors fixed**             | 3     | `controller.quorum.voters` deprecation version (3.4→4.1), `message.max.bytes` default (1000000→1048588), KRaft config example updated to `controller.quorum.bootstrap.servers`                                 |
| **Outdated content updated** | 3     | Java version matrix (added Java 25, Java 8 removal), KRaft provisioning workflow (3 new flags + dynamic add/remove), Tiered Storage configs (3 new 4.3 properties + 1 deprecation)                             |
| **New 4.3 features added**   | 7     | Broker cordoning (KIP-1066), partition size metrics (KIP-1257), KRaft fetch controls (KIP-1219), ELR note, classic rebalance deprecation (KIP-1274), OAuth client assertions (KIP-1258), share groups gap note |
| **Checklist items added**    | 4     | `controller.quorum.bootstrap.servers`, Java 17+ confirmation, cordoning workflow, rebalance migration plan                                                                                                     |
| **Appendix entries added**   | 10    | Fixes #10–#19 in Appendix A; Gaps #6–#8 in Appendix B                                                                                                                                                          |
