# Apache Kafka - Project Structure & Dependency Summary

> Generated from `build.gradle`, `settings.gradle`, `gradle/dependencies.gradle`, and `gradle.properties`.

---

## 1. Project Metadata

| Property          | Value                                                 |
| ----------------- | ----------------------------------------------------- |
| **Group**         | `org.apache.kafka`                                    |
| **Version**       | `4.4.0-SNAPSHOT`                                      |
| **Scala Version** | `2.13.18` (base: `2.13`)                              |
| **Swagger**       | `2.2.48`                                              |
| **Gradle**        | `9.4.1` (wrapper)                                     |
| **Build JDK**     | 17+ (most modules); 11+ for clients/generator/streams |

---

## 2. Build System & Plugins

### Gradle Plugins (root project)

| Plugin                                     | Version           | Purpose                                                          |
| ------------------------------------------ | ----------------- | ---------------------------------------------------------------- |
| `com.github.ben-manes.versions`            | 0.54.0            | Dependency version checks                                        |
| `idea`                                     | —                 | IntelliJ IDEA integration                                        |
| `jacoco`                                   | —                 | Java code coverage (disabled by default)                         |
| `java-library`                             | —                 | Java library conventions                                         |
| `org.owasp.dependencycheck`                | 12.2.1            | OWASP dependency vulnerability scan                              |
| `org.nosphere.apache.rat`                  | 0.8.1             | Apache Release Audit Tool (license headers)                      |
| `io.swagger.core.v3.swagger-gradle-plugin` | ${swaggerVersion} | OpenAPI/Swagger doc generation                                   |
| `com.github.spotbugs`                      | 6.5.1             | Static analysis (applied `false`)                                |
| `org.scoverage`                            | 8.1               | Scala code coverage (applied `false`)                            |
| `com.gradleup.shadow`                      | 9.4.1             | Fat JAR (shadow) (applied `false`)                               |
| `com.diffplug.spotless`                    | 8.4.0             | Code formatting                                                  |
| `org.apache.kafka.public-api-checker`      | —                 | Public API compliance checker (via included build `api-checker`) |

### Develocity (Build Scans)

| Plugin                                             | Version |
| -------------------------------------------------- | ------- |
| `com.gradle.develocity`                            | 3.19    |
| `com.gradle.common-custom-user-data-gradle-plugin` | 2.0.2   |

Build scans published to `https://develocity.apache.org` (tagged as `github` or `local`, plus current JDK version).

---

## 3. Module Structure (Included Projects from `settings.gradle`)

### Client Libraries

| Module Path                          | Archive Name                      | Description                                                                                                                                |
| ------------------------------------ | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `:clients`                           | `kafka-clients`                   | Producer, consumer, admin client APIs & protocol messages. Uses **shadow** plugin to produce a fat JAR with shaded protobuf/opentelemetry. |
| `:clients:clients-integration-tests` | `kafka-clients-integration-tests` | Integration tests for clients module.                                                                                                      |

### Broker / Server Core

| Module Path      | Archive Name          | Description                                                        |
| ---------------- | --------------------- | ------------------------------------------------------------------ |
| `:core`          | `kafka_${baseScala}`  | Broker runtime, log, replication, request handling (Scala + Java). |
| `:server`        | `kafka-server`        | Server components.                                                 |
| `:server-common` | `kafka-server-common` | Shared server code.                                                |
| `:metadata`      | `kafka-metadata`      | Metadata layer (KRaft).                                            |
| `:raft`          | `kafka-raft`          | Raft consensus implementation.                                     |

### Storage

| Module Path    | Archive Name                                     | Description                                |
| -------------- | ------------------------------------------------ | ------------------------------------------ |
| `:storage`     | `kafka-storage`                                  | Log segments, checkpoints, tiered storage. |
| `:storage:api` | `kafka-storage-api` (renamed from `storage-api`) | Storage API interfaces.                    |

### Coordinators

| Module Path                                | Archive Name                    | Description                   |
| ------------------------------------------ | ------------------------------- | ----------------------------- |
| `:coordinator-common`                      | `kafka-coordinator-common`      | Shared coordinator code.      |
| `:group-coordinator`                       | `kafka-group-coordinator`       | Consumer group coordinator.   |
| `:group-coordinator:group-coordinator-api` | `kafka-group-coordinator-api`   | Group coordinator public API. |
| `:transaction-coordinator`                 | `kafka-transaction-coordinator` | Transaction coordinator.      |
| `:share-coordinator`                       | `kafka-share-coordinator`       | Share (KIP-932) coordinator.  |

### Kafka Streams

| Module Path                       | Archive Name                       | Description                                           |
| --------------------------------- | ---------------------------------- | ----------------------------------------------------- |
| `:streams`                        | `kafka-streams`                    | Kafka Streams DSL & processor API.                    |
| `:streams:streams-scala`          | `kafka-streams-scala_${baseScala}` | Scala wrapper for Kafka Streams.                      |
| `:streams:test-utils`             | `kafka-streams-test-utils`         | Test utilities for Streams.                           |
| `:streams:integration-tests`      | `kafka-streams-integration-tests`  | Integration tests for Streams.                        |
| `:streams:examples`               | `kafka-streams-examples`           | Streams example applications.                         |
| `:streams:upgrade-system-tests-*` | (various)                          | Upgrade system tests from Kafka 0.11.0 through 4.3.x. |

### Kafka Connect

| Module Path                     | Archive Name | Description                                |
| ------------------------------- | ------------ | ------------------------------------------ |
| `:connect:api`                  | —            | Connect API interfaces.                    |
| `:connect:basic-auth-extension` | —            | Basic auth extension for Connect REST API. |
| `:connect:file`                 | —            | File source/sink connector.                |
| `:connect:json`                 | —            | JSON converter.                            |
| `:connect:mirror`               | —            | MirrorMaker 2.0.                           |
| `:connect:mirror-client`        | —            | MirrorMaker 2.0 client.                    |
| `:connect:runtime`              | —            | Connect runtime engine.                    |
| `:connect:test-plugins`         | —            | Test plugins for Connect.                  |
| `:connect:transforms`           | —            | Connect SMTs (Single Message Transforms).  |

### Tools & Shell

| Module Path        | Archive Name      | Description                                             |
| ------------------ | ----------------- | ------------------------------------------------------- |
| `:tools`           | `kafka-tools`     | CLI tools (kafka-topics, kafka-console-producer, etc.). |
| `:tools:tools-api` | `kafka-tools-api` | Tools public API.                                       |
| `:shell`           | `kafka-shell`     | Kafka metadata shell.                                   |

### Other Modules

| Module Path       | Archive Name      | Description                                       |
| ----------------- | ----------------- | ------------------------------------------------- |
| `:generator`      | —                 | RPC/message code generation (`MessageGenerator`). |
| `:examples`       | `kafka-examples`  | Kafka producer/consumer examples.                 |
| `:jmh-benchmarks` | — (not published) | JMH benchmark suite.                              |
| `:trogdor`        | `trogdor`         | Fault injection / test framework.                 |

### Test Common

| Module Path                             | Archive Name                     | Description                                                |
| --------------------------------------- | -------------------------------- | ---------------------------------------------------------- |
| `:test-common:test-common-internal-api` | `kafka-test-common-internal-api` | Internal test APIs (Java 17+).                             |
| `:test-common:test-common-util`         | `kafka-test-common-util`         | Runtime JUnit extensions (Java 11+).                       |
| `:test-common:test-common-runtime`      | `kafka-test-common-runtime`      | Runtime JUnit extensions for integration tests (Java 17+). |

**Total: ~50+ subprojects** across 11 top-level groups.

---

## 4. Dependency Version Catalog

All version and library coordinates are defined in `gradle/dependencies.gradle` under `ext.versions` and `ext.libs`.

### Key Version Highlights

| Dependency         | Version     |
| ------------------ | ----------- |
| **Scala**          | 2.13.18     |
| **Jackson**        | 2.21.5      |
| **Jetty**          | 12.0.34     |
| **Jersey**         | 3.1.10      |
| **JUnit**          | 5.14.3      |
| **JUnit Platform** | 1.14.3      |
| **Mockito**        | 5.23.0      |
| **Log4j 2**        | 2.25.5      |
| **SLF4J**          | 1.7.36      |
| **RocksDB**        | 10.1.3      |
| **Zstd**           | 1.5.6-10    |
| **LZ4 (lz4-java)** | 1.10.2      |
| **Snappy**         | 1.1.10.7    |
| **Caffeine**       | 3.2.0       |
| **Testcontainers** | 1.20.2      |
| **Checkstyle**     | 12.3.1      |
| **SpotBugs**       | 4.9.8       |
| **JMH**            | 1.37        |
| **Zinc**           | 1.12.0      |
| **protobuf**       | 3.25.5      |
| **OpenTelemetry**  | 1.3.2-alpha |
| **BouncyCastle**   | 1.84        |
| **JGit**           | 7.6.0       |
| **HdrHistogram**   | 2.2.2       |

### Library Categories

**Compression:**

- `com.github.luben:zstd-jni:1.5.6-10`
- `at.yawk.lz4:lz4-java:1.10.2`
- `org.xerial.snappy:snappy-java:1.1.10.7`

**Serialization / JSON:**

- Jackson stack: `jackson-databind`, `jackson-dataformat-csv`, `jackson-dataformat-yaml`, `jackson-datatype-jdk8`, `jackson-module-blackbird`, `jackson-jakarta-rs-json-provider`, `jackson-annotations`

**Logging:**

- `org.slf4j:slf4j-api:1.7.36`
- `org.apache.logging.log4j:log4j-api:2.25.5`
- `org.apache.logging.log4j:log4j-core:2.25.5`
- `org.apache.logging.log4j:log4j-1.2-api:2.25.5` (Log4j 1.x bridge)
- `org.apache.logging.log4j:log4j-slf4j-impl:2.25.5`

**HTTP / REST / Embedded Server:**

- Jetty 12 (`jetty-server`, `jetty-client`, `jetty-ee10-servlet`, `jetty-ee10-servlets`)
- Jersey 3 (`jersey-container-servlet`, `jersey-hk2`)
- Swagger (`swagger-annotations`, `swagger-jaxrs2-jakarta`)

**Testing:**

- JUnit 5 (Jupiter + Platform Suite Engine + Platform Launcher)
- Mockito (mockito-core + mockito-junit-jupiter)
- Hamcrest 3.0
- Testcontainers (JUnit Jupiter + Keycloak)
- Mock OAuth2 Server

**Storage:**

- `org.rocksdb:rocksdbjni:10.1.3` (Streams state stores)
- `com.github.ben-manes.caffeine:caffeine:3.2.0` (caching)

**Metrics:**

- `com.yammer.metrics:metrics-core:2.2.0`

**Security:**

- `org.bouncycastle:bcpkix-jdk18on:1.84`
- `org.bitbucket.b_c:jose4j:0.9.6`
- ApacheDS Kerberos stack

**Code Generation:**

- `argparse4j:0.7.0`
- JGit 7.6.0 (for git operations in generator)
- JMH 1.37 (benchmark annotation processor)

---

## 5. Module Dependency Graph (Key Inter-Module Relationships)

```
clients (fat jar via shadow)
├── zstd, lz4, snappy (shaded)
├── opentelemetry-proto, protobuf (shaded/relocated)
└── slf4j-api (shadowed runtime dep)

server-common
└── clients

metadata
├── server-common
├── clients
└── raft

raft
├── server-common
├── clients
└── storage

storage
├── storage-api
├── server-common
├── clients
└── caffeine

server
├── clients, metadata, server-common, storage, raft
├── group-coordinator, transaction-coordinator, share-coordinator
└── storage-api

core (Scala + Java)
├── clients, server-common, server, metadata, raft, storage
├── group-coordinator, transaction-coordinator, share-coordinator
├── coordinator-common, tools-api
└── scala-library, scala-logging, scala-reflect

group-coordinator
├── server-common, clients, metadata, storage, coordinator-common
└── hash4j

transaction-coordinator
├── clients, server-common, coordinator-common
└── jackson-databind

share-coordinator
├── clients, coordinator-common, metadata, server-common
└── metrics

coordinator-common
├── clients, server-common, metadata, storage
└── hdrHistogram, metrics

streams
├── clients (via shadow configuration)
├── rocksdbjni (public API via RocksDBConfigSetter)
└── jackson-annotations, jackson-databind

tools
├── clients, metadata, storage, server, server-common
├── connect:runtime, tools-api
├── raft, group-coordinator, coordinator-common, share-coordinator
└── transaction-coordinator

trogdor
├── clients, group-coordinator
├── Jetty server, Jersey, Jackson
└── argparse4j

shell
├── server-common, clients, core, metadata, raft, server
├── jline
└── argparse4j, jackson-databind
```

---

## 6. Java Version Requirements

| Module Group                    | Minimum Java Version |
| ------------------------------- | -------------------- |
| `:clients`                      | 11                   |
| `:generator`                    | 11                   |
| `:streams`                      | 11                   |
| `:streams:test-utils`           | 11                   |
| `:streams:examples`             | 11                   |
| `:streams:streams-scala`        | 11                   |
| `:test-common:test-common-util` | 11                   |
| All other modules               | 17                   |

---

## 7. Code Quality & Verification

| Tool                   | Configuration                                                               | Enforced on                               |
| ---------------------- | --------------------------------------------------------------------------- | ----------------------------------------- |
| **Checkstyle**         | `checkstyle/checkstyle.xml` + module-specific `import-control-*.xml`        | Main + Test Java sources                  |
| **SpotBugs**           | `gradle/spotbugs-exclude.xml`                                               | Main sources                              |
| **Spotless**           | Java import order (`kafka → org.apache.kafka → ...`); Scala scalafmt        | Java + Scala sources                      |
| **JaCoCo**             | Code coverage (disabled by default, opt-in via `-PenableTestCoverage=true`) | All modules except `:core` uses Scoverage |
| **Scoverage**          | Scala code coverage for `:core` module                                      | Core (Scala)                              |
| **OWASP**              | `gradle/resources/dependencycheck-suppressions.xml`                         | All except `jmh-benchmarks`, `trogdor`    |
| **RAT**                | License header audit (excludes git-ignored files)                           | Entire repo                               |
| **Public API Checker** | Via included `:api-checker` build                                           | Each module's JAR surface                 |

---

## 8. Test Infrastructure

### Test Types

- **`test`** — Runs all tests via JUnit Platform
- **`unitTest`** — Excludes tests tagged `integration`
- **`integrationTest`** — Only tests tagged `integration`
- **`testAll`** (streams) — Runs all Streams subproject tests

### Test Configuration

- **Parallel forks**: defaults to available processors
- **Max heap**: 3g (test), 2560m (integrationTest), 2g (unitTest)
- **JVM args**: `-Xss4m -XX:+UseParallelGC` + `--add-opens` for JDK 16+
- **Test retries**: configurable via `-PmaxTestRetries` / `-PmaxTestRetryFailures`
- **Flaky tests**: opt-in via `-Pkafka.test.run.flaky=true`
- **Mockito agent**: auto-attached when `mockito-core` is a dependency (Java 21+ workaround)
- **Per-test stdout logging**: captured per method; only retained for failed tests

---

## 9. Distribution (releaseTarGz)

The `releaseTarGz` task in `:core` assembles the full Kafka distribution tarball:

```
kafka_${baseScala}-${version}/
├── bin/              # Shell scripts
├── config/           # Sample configurations
├── libs/             # All runtime JARs (core, clients, connect, streams, tools, trogdor, shell, dependencies)
├── licenses/         # License files
├── site-docs/        # Generated documentation
├── LICENSE-binary    → LICENSE
└── NOTICE-binary     → NOTICE
```

---

## 10. Key Build Properties

| Property (gradle.properties) | Value                              |
| ---------------------------- | ---------------------------------- |
| `group`                      | `org.apache.kafka`                 |
| `version`                    | `4.4.0-SNAPSHOT`                   |
| `scalaVersion`               | `2.13.18`                          |
| `swaggerVersion`             | `2.2.48`                           |
| `org.gradle.jvmargs`         | `-Xmx4g -Xss4m -XX:+UseParallelGC` |
| `org.gradle.parallel`        | `true`                             |

### Useful Command-Line Properties

| Property                      | Purpose                                                                        |
| ----------------------------- | ------------------------------------------------------------------------------ |
| `-PmaxParallelForks=N`        | Set test fork parallelism                                                      |
| `-PignoreFailures=true`       | Continue tests on failure                                                      |
| `-PmaxTestRetries=N`          | Retry failed tests                                                             |
| `-PenableTestCoverage=true`   | Enable code coverage                                                           |
| `-PskipSigning=true`          | Skip Maven artifact signing                                                    |
| `-Pkafka.test.run.flaky=true` | Include flaky tests                                                            |
| `-PscalaOptimizerMode`        | Scala compiler optimization (`none`, `method`, `inline-kafka`, `inline-scala`) |
| `-PkeepAliveMode`             | Scala compile keep-alive (`daemon`, `session`)                                 |
