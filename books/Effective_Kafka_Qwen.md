# Effective Kafka

**A Hands-On Guide to Building Robust and Scalable Event-Driven Applications with Code Examples in Java**

**Author:** Emil Koutanov  
**Published:** 2021-01-05  
**Source:** http://leanpub.com/effectivekafka  
**Copyright:** © 2019-2021 Emil Koutanov

---

## Contents

- [Effective Kafka](#effective-kafka)
  - [Contents](#contents)
- [Chapter 1: Event Streaming Fundamentals](#chapter-1-event-streaming-fundamentals)
  - [The real challenges of distributed systems](#the-real-challenges-of-distributed-systems)
    - [Coupling](#coupling)
    - [Resilience](#resilience)
    - [Consistency](#consistency)
  - [Event-Driven Architecture](#event-driven-architecture)
    - [Coupling](#coupling-1)
    - [Resilience](#resilience-1)
    - [Consistency](#consistency-1)
    - [Applicability](#applicability)
  - [What is event streaming?](#what-is-event-streaming)
- [Chapter 2: Introducing Apache Kafka](#chapter-2-introducing-apache-kafka)
  - [The history of Kafka](#the-history-of-kafka)
  - [The present day](#the-present-day)
  - [Uses of Kafka](#uses-of-kafka)
    - [Publish-subscribe](#publish-subscribe)
    - [Log aggregation](#log-aggregation)
    - [Log shipping](#log-shipping)
    - [SEDA pipelines](#seda-pipelines)
    - [CEP](#cep)
    - [Event-sourced CQRS](#event-sourced-cqrs)
- [Chapter 3: Architecture and Core Concepts](#chapter-3-architecture-and-core-concepts)
  - [Architecture Overview](#architecture-overview)
    - [Broker nodes](#broker-nodes)
    - [ZooKeeper nodes](#zookeeper-nodes)
    - [Producers](#producers)
    - [Consumers](#consumers)
  - [Total and partial order](#total-and-partial-order)
  - [Records](#records)
  - [Partitions](#partitions)
  - [Topics](#topics)
  - [Consumer groups and load balancing](#consumer-groups-and-load-balancing)
    - [Committing offsets](#committing-offsets)
  - [Free consumers](#free-consumers)
  - [Summary of core concepts](#summary-of-core-concepts)
- [Chapter 4: Installation](#chapter-4-installation)
  - [Installing Kafka and ZooKeeper](#installing-kafka-and-zookeeper)
  - [Launching Kafka and ZooKeeper](#launching-kafka-and-zookeeper)
  - [Running in the background](#running-in-the-background)
  - [Installing Kafdrop](#installing-kafdrop)
- [Chapter 5: Getting Started](#chapter-5-getting-started)
  - [Publishing and consuming using the CLI](#publishing-and-consuming-using-the-cli)
    - [Creating a topic](#creating-a-topic)
    - [Publishing records](#publishing-records)
    - [Consuming records](#consuming-records)
  - [Useful CLI commands](#useful-cli-commands)
    - [Listing topics](#listing-topics)
    - [Describing a topic](#describing-a-topic)
    - [Deleting a topic](#deleting-a-topic)
    - [Truncating partitions](#truncating-partitions)
    - [Listing consumer groups](#listing-consumer-groups)
    - [Describing a consumer group](#describing-a-consumer-group)
    - [Resetting offsets](#resetting-offsets)
    - [Deleting offsets](#deleting-offsets)
    - [Deleting a consumer group](#deleting-a-consumer-group)
  - [A basic Java producer and consumer](#a-basic-java-producer-and-consumer)
    - [Client libraries](#client-libraries)
    - [Using the Java library](#using-the-java-library)
    - [Publishing records](#publishing-records-1)
    - [Consuming records](#consuming-records-1)
- [Chapter 6: Design Considerations](#chapter-6-design-considerations)
  - [Roles and responsibilities](#roles-and-responsibilities)
    - [Event-oriented broadcast](#event-oriented-broadcast)
    - [Peer-to-peer messaging](#peer-to-peer-messaging)
    - [Topic conditioning](#topic-conditioning)
  - [Parallelism](#parallelism)
    - [Producer-driven partitioning](#producer-driven-partitioning)
    - [Topic width](#topic-width)
    - [Scaling of the consumer group](#scaling-of-the-consumer-group)
    - [Internal consumer parallelism](#internal-consumer-parallelism)
  - [Idempotence and exactly-once delivery](#idempotence-and-exactly-once-delivery)
- [Chapter 7: Serialization](#chapter-7-serialization)
  - [Key and value serializer](#key-and-value-serializer)
  - [Sending events](#sending-events)
    - [The complete sender](#the-complete-sender)
  - [Key and value deserializer](#key-and-value-deserializer)
  - [Receiving events](#receiving-events)
    - [Corrupt records](#corrupt-records)
    - [The complete receiver](#the-complete-receiver)
  - [Pipelining](#pipelining)
  - [Record filtering](#record-filtering)
- [Chapter 8: Bootstrapping and Advertised Listeners](#chapter-8-bootstrapping-and-advertised-listeners)
  - [A gentle introduction to bootstrapping](#a-gentle-introduction-to-bootstrapping)
    - [Taking advantage of DNS](#taking-advantage-of-dns)
  - [A simple scenario](#a-simple-scenario)
  - [Multiple listeners](#multiple-listeners)
  - [Listeners and the Docker Network](#listeners-and-the-docker-network)
- [Chapter 9: Broker Configuration](#chapter-9-broker-configuration)
  - [Entity types](#entity-types)
  - [Dynamic update modes](#dynamic-update-modes)
  - [Configuration precedence and defaults](#configuration-precedence-and-defaults)
  - [Applying broker configuration](#applying-broker-configuration)
    - [Static configuration](#static-configuration)
    - [Dynamic configuration](#dynamic-configuration)
  - [Applying topic configuration](#applying-topic-configuration)
  - [Users and Clients](#users-and-clients)
- [Chapter 10: Client Configuration](#chapter-10-client-configuration)
  - [Configuration gotchas](#configuration-gotchas)
  - [Applying client configuration](#applying-client-configuration)
  - [Common configuration](#common-configuration)
    - [Bootstrap servers](#bootstrap-servers)
    - [Client DNS lookup](#client-dns-lookup)
    - [Client ID](#client-id)
    - [Retries and retry backoff](#retries-and-retry-backoff)
      - [Testing considerations](#testing-considerations)
    - [Security configuration](#security-configuration)
  - [Producer configuration](#producer-configuration)
    - [Acknowledgements](#acknowledgements)
    - [Maximum in-flight requests per connection](#maximum-in-flight-requests-per-connection)
    - [Enable idempotence](#enable-idempotence)
    - [Compression type](#compression-type)
    - [Key and value serializer](#key-and-value-serializer-1)
    - [Partitioner](#partitioner)
    - [Interceptors](#interceptors)
    - [Maximum block time](#maximum-block-time)
    - [Batch size and linger time](#batch-size-and-linger-time)
    - [Request timeout](#request-timeout)
    - [Delivery timeout](#delivery-timeout)
    - [Transactional ID and transaction timeout](#transactional-id-and-transaction-timeout)
  - [Consumer configuration](#consumer-configuration)
    - [Key and value deserializer](#key-and-value-deserializer-1)
    - [Interceptors](#interceptors-1)
    - [Controlling the fetch size](#controlling-the-fetch-size)
    - [Group ID](#group-id)
    - [Group instance ID](#group-instance-id)
    - [Heartbeat interval, session timeout, and the maximum poll interval](#heartbeat-interval-session-timeout-and-the-maximum-poll-interval)
    - [Auto offset reset](#auto-offset-reset)
    - [Enable auto-commit and the auto-commit interval](#enable-auto-commit-and-the-auto-commit-interval)
    - [Partition assignment strategy](#partition-assignment-strategy)
    - [Transactions](#transactions)
  - [Admin client configuration](#admin-client-configuration)
- [Chapter 11: Robust Configuration](#chapter-11-robust-configuration)
  - [Using constants](#using-constants)
  - [Type-safe configuration](#type-safe-configuration)
- [Chapter 12: Batching and Compression](#chapter-12-batching-and-compression)
  - [Comparing disk and network I/O](#comparing-disk-and-network-io)
  - [Producer record batching](#producer-record-batching)
  - [Compression](#compression)
- [Chapter 13: Replication and Acknowledgements](#chapter-13-replication-and-acknowledgements)
  - [Replication basics](#replication-basics)
    - [In-Sync Replicas (ISR)](#in-sync-replicas-isr)
  - [Leader election](#leader-election)
  - [Setting the initial replication factor](#setting-the-initial-replication-factor)
  - [Changing the replication factor](#changing-the-replication-factor)
  - [Decommissioning broker nodes](#decommissioning-broker-nodes)
  - [Acknowledgements](#acknowledgements-1)
    - [No acknowledgements (`acks=0`)](#no-acknowledgements-acks0)
    - [One acknowledgement (`acks=1`)](#one-acknowledgement-acks1)
    - [All acknowledgements (`acks=all` or `acks=-1`)](#all-acknowledgements-acksall-or-acks-1)
- [Chapter 14: Data Retention](#chapter-14-data-retention)
  - [Kafka storage internals](#kafka-storage-internals)
    - [Organisation of log data](#organisation-of-log-data)
    - [Indexes](#indexes)
    - [Rotation of log segments](#rotation-of-log-segments)
  - [Deletion](#deletion)
  - [Compaction](#compaction)
    - [Use cases behind compaction](#use-cases-behind-compaction)
    - [Overwriting and deleting records](#overwriting-and-deleting-records)
    - [Behind the scenes](#behind-the-scenes)
  - [Combining compaction with deletion](#combining-compaction-with-deletion)
- [Chapter 15: Group Membership and Partition Assignment](#chapter-15-group-membership-and-partition-assignment)
  - [Group membership basics](#group-membership-basics)
    - [Establishing group membership](#establishing-group-membership)
    - [State synchronisation](#state-synchronisation)
    - [Delayed rebalance](#delayed-rebalance)
    - [State synchronisation barrier](#state-synchronisation-barrier)
    - [Incremental cooperative rebalancing](#incremental-cooperative-rebalancing)
    - [Static membership](#static-membership)
  - [Liveness and safety](#liveness-and-safety)
    - [Dealing with failures](#dealing-with-failures)
    - [Dealing with partition exclusivity](#dealing-with-partition-exclusivity)
  - [Partition assignment strategy](#partition-assignment-strategy-1)
    - [Built-in assignors](#built-in-assignors)
    - [Upgrading assignors](#upgrading-assignors)
- [Chapter 16: Security](#chapter-16-security)
  - [State of security in Kafka](#state-of-security-in-kafka)
  - [Target state security](#target-state-security)
  - [Network traffic policy](#network-traffic-policy)
  - [Confidentiality](#confidentiality)
    - [Client-to-broker encryption](#client-to-broker-encryption)
    - [Interbroker encryption](#interbroker-encryption)
    - [Broker-to-ZooKeeper encryption](#broker-to-zookeeper-encryption)
    - [Encryption at rest](#encryption-at-rest)
  - [Authentication](#authentication)
    - [Mutual TLS (mTLS)](#mutual-tls-mtls)
    - [SASL (Simple Authentication and Security Layer)](#sasl-simple-authentication-and-security-layer)
      - [SASL/GSSAPI (Kerberos)](#saslgssapi-kerberos)
      - [SASL/SCRAM](#saslscram)
      - [Interbroker authentication](#interbroker-authentication)
      - [External JAAS configuration](#external-jaas-configuration)
      - [OAuth 2.0 bearer tokens](#oauth-20-bearer-tokens)
      - [Delegation tokens](#delegation-tokens)
    - [ZooKeeper authentication](#zookeeper-authentication)
    - [Configuring CLI tools and Kafdrop](#configuring-cli-tools-and-kafdrop)
  - [Authorization](#authorization)
    - [Enable authorization](#enable-authorization)
    - [Managing ACLs](#managing-acls)
    - [World-readable topics and mixing allow/deny](#world-readable-topics-and-mixing-allowdeny)
    - [Prefixed resource patterns](#prefixed-resource-patterns)
    - [Listing and bulk-removal](#listing-and-bulk-removal)
    - [Network address restrictions](#network-address-restrictions)
    - [Common authorization scenarios](#common-authorization-scenarios)
- [Chapter 17: Quotas](#chapter-17-quotas)
  - [The rationale behind quotas](#the-rationale-behind-quotas)
    - [Mitigating denial of service attacks](#mitigating-denial-of-service-attacks)
    - [Capacity planning](#capacity-planning)
    - [Quality of service](#quality-of-service)
  - [Types of quotas](#types-of-quotas)
    - [Network bandwidth quotas](#network-bandwidth-quotas)
    - [Request rate quotas](#request-rate-quotas)
  - [Subject affinity and precedence order](#subject-affinity-and-precedence-order)
  - [Applying quotas](#applying-quotas)
    - [Buffering and timeouts](#buffering-and-timeouts)
    - [Sensing quota enforcement](#sensing-quota-enforcement)
    - [Tuning the duration and number of sampling windows](#tuning-the-duration-and-number-of-sampling-windows)
- [Chapter 18: Transactions](#chapter-18-transactions)
  - [Preamble](#preamble)
  - [The rationale behind transactions](#the-rationale-behind-transactions)
    - [The problem: Duplicate records](#the-problem-duplicate-records)
    - [The solution: Transactions](#the-solution-transactions)
  - [Transactions under the hood](#transactions-under-the-hood)
    - [Role of the transaction coordinator](#role-of-the-transaction-coordinator)
    - [Producer API enhancements](#producer-api-enhancements)
    - [Assigning a transactional ID](#assigning-a-transactional-id)
    - [Transactional consumers](#transactional-consumers)
  - [Simple stream processing example](#simple-stream-processing-example)
  - [Limitations](#limitations)
  - [Are transactions over-hyped?](#are-transactions-over-hyped)
- [Appendix A: Modern Kafka Migration](#appendix-a-modern-kafka-migration)
  - [A.1 KRaft: ZooKeeper Is Gone](#a1-kraft-zookeeper-is-gone)
  - [A.2 Consumer Rebalance Protocol: KIP-848](#a2-consumer-rebalance-protocol-kip-848)
  - [A.3 Share Groups (KIP-932)](#a3-share-groups-kip-932)
  - [A.4 Streams Rebalance Protocol (KIP-1071)](#a4-streams-rebalance-protocol-kip-1071)
  - [A.5 Kafka Streams Scala Library Deprecation](#a5-kafka-streams-scala-library-deprecation)
  - [A.6 Tiered Storage](#a6-tiered-storage)
  - [A.7 Java Version Requirements](#a7-java-version-requirements)
  - [A.8 Dynamic KRaft Controllers (KIP-853)](#a8-dynamic-kraft-controllers-kip-853)
  - [A.9 Eligible Leader Replicas (ELR)](#a9-eligible-leader-replicas-elr)
  - [A.10 Broker Cordoning (KIP-1066)](#a10-broker-cordoning-kip-1066)
  - [A.11 CLI Tool Changes](#a11-cli-tool-changes)
  - [A.12 Config Deprecations and Removals](#a12-config-deprecations-and-removals)
  - [A.13 Summary: Book-to-Modern Mapping](#a13-summary-book-to-modern-mapping)

---

# Chapter 1: Event Streaming Fundamentals

It is amazing how the software engineering landscape has transformed over the last decade. Not long ago, applications were largely monolithic in nature, internally-layered, typically hosted within application servers and backed by “big iron” relational databases with hundreds or thousands of interrelated tables. Distributed applications were the “gold standard” by those measures — coarse-grained deployable units scattered among a static cluster of application servers, hosted on a fleet of virtual machines and communicating over SOAP-based APIs or message queues. Containerisation, cloud computing, elasticity, ephemeral computing, functions-as-a-service, immutable infrastructure — all niche concepts that were just starting to surface, making minor, barely perceptible ripples in an architectural institution that was otherwise well-set in its ways.

That was then. Today, these concepts are profoundly commonplace. Engineers are often heard interleaving several such terms in the same sentence; it would seem that the engineering community had miraculously stumbled upon an elixir that has all but cured us of our prior burdens — at least when it comes to developer velocity, time-to-market, system availability, scalability, and just about every other material concern that had kept the engineering manager of yore awake at night. Today, we have microservices in the cloud. Problem solved. Next question.

Except no such event actually occurred. We did not discover a solution to the problem; we merely shifted the problem. Aspects of software development that used to be straightforward in the “old world”, such as debugging, profiling, performance management, and state consistency — are now an order of magnitude more complex. On top of this, a microservices architecture brings its own unique woes. Services are more fluid and elastic, and tracking of their instances, their versions and dependencies is a Herculean challenge that balloons in complexity as the component landscape evolves. To top this off, services will fail in isolation, further exacerbated by unreliable networks, potentially leaving some activities in a state of partial completeness. Given a large enough system, parts of it may be suffering a minor outage at any given point in time, potentially impacting a subset of users, quite often without the operator’s awareness.

With so many “moving parts”, how does one stay on top of these challenges? How does one make the engineering process sustainable? Or should we just write off the metamorphosis of the recent decade as a failed experiment?

## The real challenges of distributed systems

If there is one thing to be learned from the opening gambit, it is that there is no “silver bullet”. Architectural paradigms are somewhat like design patterns, but broader scoped, more subjective, and far less prescriptive. However fashionable and blogged-about these paradigms might be, they only offer partial solutions to common problems. One must be mindful of the context at all times, and apply the model judiciously. And crucially, one must understand the deficiencies of the proposed approach, being able to reason about the implications of its adoption — both immediate and long-term.

The principal inconvenience of a distributed system is that it shifts the complexity from the innards of a service implementation to the notional fabric that spans across services. Some might say, it lifts the complexity from the micro level to the macro level. In doing so, it does not reduce the net complexity; on the contrary, it increases the aggregate complexity of the combined solution. An astute engineering leader is well-aware of this. The reason why a distributed architecture is often chosen — assuming it is chosen correctly — is to enable the compartmentalisation of the problem domain. It can, if necessary, be decomposed into smaller chunks and solved in partial isolation, typically by different teams — then progressively integrated into a complete whole. In some cases, this decomposition is deliberate, where teams are organised around the problem. In other, less-than-ideal cases, the breakdown of the problem is a reflection of Conway’s Law, conforming to organisational structures. Presumed is the role an architect, or a senior engineering figure that orchestrates the decomposition, assuming the responsibility for ensuring the conceptual integrity and efficacy of the overall solution. Centralised coordination may not always be present — some organisations have opted for a more democratic style, whereby teams act in concert to decompose the problem organically, with little outside influence.

### Coupling

Whichever the style of decomposition, the notion of macro complexity cannot be escaped. Fundamentally, components must communicate in one manner or another, and therein lies the problem: components are often inadvertently made aware of each other. This is called **coupling** — the degree of interdependence between software components. The lower the coupling, the greater the propensity of the system to evolve to meet new requirements, performance demands, and operational challenges. Conversely, tight coupling shackles the components of the system, increasing their mutual reliance and impeding their evolution.

There are known ways for alleviating the problem of coupling, such as the use of an asynchronous communication style and message-oriented middleware to segregate components. These techniques have been used to varying degrees of success; there are times where message-based communication has created a false economy — collaborating components may still be transitively dependent upon one another in spite of their designers’ best efforts to forge opaque conduits between them.

### Resilience

It would be rather nice if computers never failed and networks were reliable; as it happens, reality differs. The problem is exacerbated in a distributed context: the likelihood of any one component experiencing an isolated failure increases with the total number of components, which carries negative ramifications if components are interdependent.

Distributed systems typically require a different approach to resilience compared to their centralised counterparts. The quantity and makeup of failure scenarios is often much more daunting in distributed systems. Failures in centralised systems are mostly characterised as **fail-stop** scenarios — where a process fails totally and permanently, or a network partition occurs, which separates the entirety of the system from one or more clients, or the system from its dependencies. At either rate, the failure modes are trivially understood. By contrast, distributed systems introduce the concept of **partial failures**, **intermittent failures**, and, in the more extreme cases, **Byzantine failures**. The latter represents a special class of failures where processes submit incorrect or misleading information to unsuspecting peers.

### Consistency

Ensuring state consistency in a distributed system is perhaps the most difficult aspect to get right. One can think of a distributed system as a vast state machine, with some elements of it being updated independently of others. There are varying levels of consistency, and different applications may demand specific forms of consistency to satisfy their requirements. The stronger the consistency level, the more synchronisation is necessary to maintain it. Synchronisation is generally regarded as a difficult problem; it is also expensive — requiring additional resources and impacting the performance of the system.

Cost being held a constant, the greater the requirement for consistency, the less distributed a system will be. There is also a natural counterbalance between consistency and availability, identified by Eric Brewer in 1998. The essence of it is in the following: distributed systems must be tolerant of network partitions, but in achieving this tolerance, they will have to either give up consistency or availability guarantees. Note, this conjecture does not claim that a consistent system cannot simultaneously be highly available, only that it must give up availability if a network partition does occur.

By comparison, centralised systems are not bound by the same laws, as they don’t have to contend with network partitions. They can also take advantage of the underlying hardware, such as CPU cache lines and atomic operations, to ensure that individual threads within a process maintain consistency of shared data. When they do fail, they typically fail as a unit — losing any ephemeral state and leaving the persistent state as it was just before failure.

## Event-Driven Architecture

**Event-Driven Architecture** (EDA) is a paradigm promoting the production, detection, consumption of, and reaction to events. An event is a significant state change, that may be of interest within the domain where this state change occurred, or outside of that domain. Interested parties can be notified of an event by having the originating domain publish some canonical depiction of the event to a well-known conduit — a message broker, a ledger, or a shared datastore of some sort. Note, the event itself does not travel — only its notification; however, we often metonymically refer to the notification of the event as the event. While formally incorrect, it is convenient.

An event-driven system formally consists of **emitters** (also known as producers and agents), **consumers** (also known as subscribers and sinks), and **channels** (also known as brokers). We also use the term **upstream** — to refer to the elements prior to a given element in the emitter-consumer relation, and **downstream** — to refer to the subsequent elements.

An emitter of an event is not aware of any of the event’s downstream consumers. This statement captures the essence of an event-driven architecture. An emitter does not even know whether a consumer exists; every transmission of an event is effectively a “blind” broadcast. Likewise, consumers react to specific events without the knowledge of the particular emitter that published the event. A consumer need not be the final destination of the event; the event notification may be persisted or transformed by the consumer before being broadcast to the next stage in a notional pipeline. In other words, an event may spawn other events; elements in an event-driven architecture may combine the roles of emitters and consumers, simultaneously acting as both.

Event notifications are immutable. An element cannot modify an event’s representation once it has been emitted, not even if it is the original emitter. At most, it can emit new notifications relating to that event — enriching, refining, or superseding the original notification.

### Coupling

Elements within EDA are exceedingly loosely coupled, to the point that they are largely unaware of one another. Emitters and consumers are only coupled to the intermediate channels, as well as to the representations of events — schemas. While some coupling invariably remains, in practice, EDA offers the lowest degree of coupling of any practical system. The collaborating components become largely autonomous, standalone systems that operate in their own right — each with their individual set of stakeholders, operational teams, and governance mechanisms.

By way of an example, an e-commerce system might emit events for each product purchase, detailing the time, product type, quantity, the identity of the customer, and so on. Downstream of the emitter, two systems — a business intelligence (BI) platform and an enterprise resource planning (ERP) platform — might react to the sales events and build their own sets of materialised views. In effect, view-only projections of the emitter’s state. Each of these platforms are completely independent systems with their own stakeholders: the BI system satisfies the business reporting and analytics requirements for the marketing business unit, while the ERP system supports supply chain management and capacity planning — the remit of an entirely different business unit.

To put things into perspective, we shall consider the potential solutions to this problem in the absence of EDA. There are several ways one could have approached the solution; each approach commonly found in the industry to this day:

1. **Build a monolith.** Conceptually, the simplest approach, requiring a system to fulfill all requirements and cater to all stakeholders as an indivisible unit.
2. **Integration.** Allow the systems to invoke one another via some form of an API. Either the e-commerce platform could invoke the BI and ERP platforms at the point of sale, or the BI and ERP platforms could invoke the e-commerce platform APIs just before generating a business report or supplier request. Some variations of this model use message queues for systems to send commands and queries to one another.
3. **Data decapsulation.** If system integrators were cowboys, this would be their prairie. Data decapsulation, a coined term, if one were to ask, sees systems “reaching over” into each other’s “backyard”, so to speak, to retrieve data directly from the source, for example, from an SQL database — without asking the owner of the data, and oftentimes without their awareness.
4. **Shared data.** Build separate applications that share the same datastore. Each application is aware of all data, and can both read and modify any data element. Some variations of this scheme use database-level permissions to restrict access to the data based on an application’s role, thereby binding the scope of each application.

Once laid out, the drawbacks of each model become apparent. The first approach — the proverbial monolith — suffers from uncontrolled complexity growth. In effect, it has to satisfy everyone and everything. This also makes it very difficult to change. From a reliability standpoint, it is the equivalent of putting all of one’s eggs in one basket — if the monolith were to fail, it will impact all stakeholders simultaneously.

The second approach — integrate everything — is what these days is becoming more commonly known as the “distributed monolith”, especially when it is being discussed in the context of microservices. While the systems, or services, as the case may be, appear to be standalone — they might even be independently sourced and maintained — they are by no means autonomous, as they cannot change freely without impacting their peers.

The third approach — read others’ data — is the architectural equivalent of a “get rich quick scheme” that always ends in tears. It takes the path of least resistance, making it highly alluring. However, the model creates the tightest possible level of coupling, making it very difficult to change the parties down the track. It is also brittle — a minor and seemingly benign change to the internal data representation in one system could have a catastrophic effect on another system.

The final model — the use of a shared datastore — is a more civilised variation of the third approach. While it may be easier to govern, especially with the aid of database-level access control — the negative attributes are largely the same.

Now imagine that the business operates multiple disparate e-commerce platforms, located in different geographic regions or selling different sorts of products. And to top it off, the business now needs a separate data warehouse for long-term data collection and analysis. The addition of each new component significantly increases the complexity of the above solutions; in other words, they do not scale. By comparison, EDA scales perfectly linearly. Systems are unaware of one another and react to discrete events — the origin of an event is largely circumstantial. This level of autonomy permits the components to evolve rapidly in isolation, meeting new functional and non-functional requirements as necessary.

### Resilience

The autonomy created by the use of EDA ensures that, as a whole, the system is less prone to outage if any of its individual components suffer a catastrophic failure. How is this achieved?

Integrated systems, and generally, any topological arrangement that exhibits a high degree of component coupling is prone to **correlated failure** — whereby the failure of one component can take down an entire system. In a tightly coupled system, components directly rely on one another to jointly achieve some goal. If one of these components fails, then the remaining components that depend on it may also cease to function; at minimum, they will not be able to carry out those operations that depend on the failed component.

In the case of a monolith, the failure assertion is trivial — if a fail-stop scenario occurs, the entire process is affected.

Under EDA, enduring a component failure implies the inability to either emit events or consume them. In the event of emitter failure, consumers may still operate freely, albeit without a facility for reacting to new events. Using our earlier example, if the e-commerce engine fails, none of the downstream processes will be affected — the business can still run analytical queries and attend to resource planning concerns. Conversely, if the ERP system fails, the business will still make sales; however, some products might not be placed on back-order in time, potentially leading to low stock levels. Furthermore, provided the event channel is durable, the e-commerce engine will continue to publish sales events, which will eventually be processed by the ERP system when it is restored. The failure of an event channel can be countered by implementing a local, stateful buffer on the emitter, so that any backlogged events can be published when the channel has been restored. In other words, not only is an event-driven system more resilient by retaining limited operational status during component failure, it is also capable of self-healing when failed components are replaced.

In practice, systems may suffer from soft failures, where components are saturated beyond their capacity to process requests, creating a cascading effect. In networking, this phenomenon is called “congestive collapse”. In effect, components appear to be online, but are stressed — unable to turn around some fraction of requests within acceptable time frames. In turn, the requesting components — having detected a timeout — retransmit requests, hoping to eventually get a response. This increases pressure on the stressed components, exacerbating the situation. Often, the missed response is merely an indication of receiving the request — in effect, the requester is simply piling on duplicate work.

Under EDA, requesters do not require a confirmation from downstream consumers — a simple acknowledgement from the event channel is sufficient to assume that the event has been stably enqueued and that the consumer(s) will get to it at some future point in time.

### Consistency

EDA ameliorates the problem of distributed consistency by attributing explicit mastership to state, such that any stateful element can only be manipulated by at most one system — its designated owner. This is also referred to as the originating domain of the event. Other domains may only react to the event; for example, they may reduce the event stream to a local projection of the emitter’s state.

Under this model, consistency within the originating domain is trivially maintained by enforcing the single writer principle. External to the domain, the events can be replayed in the exact order they were observed on the emitter, creating **sequential consistency** — a model of consistency where updates do not have to be seen instantaneously, but must be presented in the same order to all observers, which is also the order they were observed on the emitter. Alternatively, events may be emitted in causal order, categorising them into multiple related sequences, where events within any sequence are related amongst themselves, but unrelated to events in another sequence. This is a slight relaxation of sequential consistency to allow for safe parallelism, and is sufficient in the overwhelming majority of use cases.

### Applicability

For all its outstanding benefits, EDA is not a panacea and cannot supplant integrated or monolithic systems in all cases. For instance, EDA is not well-suited to synchronous interactions, as mutual or unilateral awareness among collaborating parties runs contrary to the grain of EDA and negates most of its benefits.

EDA is not a general-purpose architectural paradigm. It is designed to be used in conjunction with other paradigms and design patterns, such as synchronous request-response style messaging, to solve more general problems. In the areas where it can be applied, it ordinarily leads to significant improvements in the system’s non-functional characteristics. Therefore, one should seek to maximise opportunities for event-driven compositions, refactoring the architecture to that extent.

## What is event streaming?

Finally, we arrive at the central question: What is event streaming? And frankly, there is little left to explain. There is but one shortfall in the earlier narrative: EDA is an architectural paradigm — it does not prescribe the particular semantics of the event interchange. Events could be broadcast among parties using different mechanisms, all potentially satisfying the basic tenets of EDA.

**Event streaming** is a mechanism that can be used to realise the event channel element in EDA. It is primarily concerned with the following aspects of event propagation:

- Interface between the emitter and the channel, and the consumer and the channel;
- Cardinality of the emitter and consumer elements that interact with a common channel;
- Delivery semantics;
- Enabling parallelism in the handling of event notifications;
- Persistence, durability, and retention of event records; and
- Ordering of events and associated consistency models.

The focal point of event streaming is, unsurprisingly, an **event stream**. At minimum, an event stream is a durable, totally-ordered, unbounded sequence of immutable event records, delivered at least once to its subscriber(s). An event streaming platform is a concrete technology that implements the event streaming model, addressing the points enumerated above. It interfaces with emitter and consumer ecosystems, hosts event streams, and may provide additional functionality beyond the essential set of event streaming capabilities. For example, an event streaming platform may offer end-to-end compression and encryption of event records, which is not essential in the construction of event-driven systems, but is convenient nonetheless.

It is worth noting that event streaming is not required to implement the event channel element of EDA. Other transports, such as message queues, may be used to fulfill similar objectives. In fact, there is nothing to say that EDA is exclusive to distributed systems; the earliest forms of EDA were realised within the confines of a single process, using purely in-memory data structures. It may seem banal in comparison, but even UI frameworks of the bygone era, such as Java Swing, draw on the foundations of EDA, as do their more contemporary counterparts, such as React.

When operating in the context of a distributed system, the primary reason for choosing event streaming over the competing alternatives is that the former was designed specifically for use in EDA, and its various implementations — event streaming platforms — offer a host of capabilities that streamline their adoption in EDA. A well-designed event streaming platform provides direct correspondence with native EDA concepts. For example, it takes care of event immutability, record ordering, and supports multiple independent consumers — concepts that might not necessarily be endemic to alternate solutions, such as message queues.

This chapter has furnished an overview of the challenges of engineering distributed systems, contrasted with the building of monolithic business applications. The numerous drawbacks of distributed systems increase their cost and complicate their upkeep. Generally speaking, the components of a complex system are distributed out of necessity — namely, the requirement to scale in both the performance plane and in the engineering capacity to deliver change.

We looked at how the state of the art has progressed since the mass adoption of the principles of distributed computing in mainstream software engineering. Specifically, we explored Event-Driven Architecture as a highly effective paradigm for reducing coupling, bolstering resilience, and avoiding the complexities of maintaining a globally consistent state.

Finally, we touched upon event streaming, which is a rendition of the event channel element of EDA. We also learned why event streaming is the preferred approach for persisting and transporting event notifications. In no uncertain terms, event streaming is the most straightforward path for the construction of event-driven systems.

---

# Chapter 2: Introducing Apache Kafka

Apache Kafka, or simply Kafka, is an event streaming platform. But it is also more than that. It is an entire ecosystem of technologies designed to assist in the construction of complete event-driven systems. Kafka goes above and beyond the essential set of event streaming capabilities, providing rich event persistence, transformation, and processing semantics.

Event streaming platforms are a comparatively recent paradigm within the broader message-oriented middleware class. There are only a handful of mainstream implementations available, compared to hundreds of MQ-style brokers, some going back to the 1980s, for example, Tuxedo. Compared to established messaging standards such as AMQP, MQTT, XMPP, and JMS, there are no equivalent standards in the streaming space. Kafka is a leader in the area of event streaming, and more broadly, event-driven architecture. While there is no de jure standard in event streaming, Kafka is the benchmark to which most competing products orient themselves. To this effect, several competitors — such as Azure Event Hubs and Apache Pulsar — offer APIs that mimic Kafka.

Event streaming platforms are an active area of continuous research and experimentation.

In spite of this, event streaming platforms aren’t just a niche concept or an academic idea with few esoteric use cases; they can be applied effectively to a broad range of messaging and eventing scenarios, routinely displacing their more traditional counterparts.

Kafka is written in Java, meaning it can run comfortably on most operating systems and hardware configurations. It can equally be deployed on bare metal, in the Cloud, and in a Kubernetes cluster. And finally, Kafka has libraries written for just about every programming language, meaning that virtually every developer can start taking advantage of event streaming and push their application architecture to the next level of resilience and scalability.

## The history of Kafka

Apache Kafka was originally developed by LinkedIn, and was subsequently open-sourced in early 2011. The name “Kafka” was chosen by one of its founders — Jay Kreps. Kreps chose to name the software after the famous 20th-century author Franz Kafka because it was “a system optimised for writing”. Kafka gained the full Apache Software Foundation project status in October 2012, having graduated from the Apache Incubator program.

Kafka was born out of a need to track and process large volumes of site events, such as page views and user actions, as well as for the aggregation of log data. Before Kafka, LinkedIn maintained several disparate data pipelines, which presented a challenge from both complexity and operational scalability perspectives. In July 2011, having consolidated the individual platforms, Kafka was processing approximately one billion events per day. By 2012, this number had risen to 20 billion. By July 2013, Kafka was carrying 200 billion events per day. Two years later, in 2015, Kafka was turning over one trillion events per day, with peaks of up to 4.5 million events per second.

Over the four years of 2011 to 2015, the volume of records has grown by three orders of magnitude. By the end of this period, LinkedIn was moving well over a petabyte of event data per week. By all means, this level of growth could not be attributed to Kafka alone; however, Kafka was undoubtedly a key enabler from an infrastructure perspective.

As of October 2019, LinkedIn maintains over 100 Kafka clusters, comprising more than 4,000 brokers. These collectively serve more than 100,000 topics and 7 million partitions. The total number of records handled by Kafka has surpassed 7 trillion per day.

## The present day

The industry adoption of Kafka has been nothing short of phenomenal. The list of tech giants that heavily rely on Kafka is impressive in itself. To name just a few:

- **Yahoo** uses Kafka for real-time analytics, handling up to 20 gigabits of uncompressed event data per second in 2015. Yahoo is also a major contributor to the Kafka ecosystem, having open-sourced its in-house Cluster Manager for Apache Kafka (CMAK) product.
- **Twitter** heavily relies on Kafka for its mobile application performance management and analytics product, which has been clocked at five billion sessions per day in February 2015. Twitter processes this stream using a combination of Apache Storm, Hadoop, and AWS Elastic MapReduce.
- **Netflix** uses Kafka as the messaging backbone for its Keystone pipeline — a unified event publishing, collection, and routing infrastructure for both batch and stream processing. As of 2016, Keystone comprises over 4,000 brokers deployed entirely in the Cloud, which collectively handle more than 700 billion events per day.
- **Tumblr** relies on Kafka as an integral part of its event processing pipeline, capturing up to 500 million page views a day back in 2012.
- **Square** uses Kafka as the underlying bus to facilitate stream processing, website activity tracking, metrics collection and monitoring, log aggregation, real-time analytics, and complex event processing.
- **Pinterest** employs Kafka for its real-time advertising platform, with 100 clusters comprising over 2,000 brokers deployed in AWS. Pinterest is turning over in excess of 800 billion events per day, peaking at 15 million per second.
- **Uber** is among the most prominent of Kafka adopters, processing in excess of a trillion events per day — mostly for data ingestion, event stream processing, database changelogs, log aggregation, and general-purpose publish-subscribe message exchanges. In addition, Uber is an avid open-source contributor — having released its in-house cluster replication solution uReplicator into the wild.

And it’s not just the engineering-focused organisations that have adopted Kafka — by some estimates, up to a third of Fortune 500 companies use Kafka to fulfill their event streaming and processing needs.

There are good reasons for this level of industry adoption. As it happens, Kafka is one of the most well-supported and well-regarded event streaming platforms, boasting an impressive number of open-source projects that integrate with Kafka. Some of the big names include Apache Storm, Apache Flink, Apache Hadoop, LogStash and the Elasticsearch Stack, to name a few. There are also Kafka Connect integrations with every major SQL database, and most NoSQL ones too. At the time of writing, there are circa one hundred supported off-the-shelf connectors, which does not include custom connectors that have been independently developed.

## Uses of Kafka

Chapter 1: Event Streaming Fundamentals has provided the necessary background, fitting Kafka as an event streaming platform within a larger event-driven system.

There are several use cases falling within the scope of EDA that are well-served by Apache Kafka. This section covers some of these scenarios, illustrating how Kafka may be used to address them.

### Publish-subscribe

Any messaging scenario where producers are generally unaware of consumers, and instead publish messages to well-known aggregations called topics. Conversely, consumers are generally unaware of the producers but are instead concerned with specific content categories. The producer and consumer ecosystems are loosely-coupled, being aware of only the common topic(s) and messaging schema(s). This pattern is commonly used in the construction of loosely-coupled microservices.

When Kafka is used for general-purpose publish-subscribe messaging, it will be competing with its “enterprise” counterparts, such as message brokers and service buses. Admittedly, Kafka might not have all the features of some of these middleware platforms — such as message deletion, priority levels, producer flow control, distributed transactions, or dead-letter queues. On the other hand, these features are mostly representative of traditional messaging paradigms — intrinsic to how these platforms are commonly used. Kafka works in its own idiomatic way — optimised around unbounded sequences of immutable events. As long as a publish-subscribe relationship can be represented as such, then Kafka is fit for the task.

### Log aggregation

Dealing with large volumes of log-structured events, typically emitted by application or infrastructure components. Logs may be generated at burst rates that significantly outstrip the ability of query-centric datastores to keep up with log ingestion and indexing, which are regarded as “expensive” operations. Kafka can act as a buffer, offering an intermediate, durable datastore. The ingestion process will act as a sink, eventually collating the logs into a read-optimised database, for example, Elasticsearch or HBase.

A log aggregation pipeline may also contain intermediate steps, each adding value en route to the final destination; for example, to compress log data, encrypt log content, normalise the logs into a canonical form, or sanitise the log entries — scrubbing them of personally-identifiable information.

### Log shipping

While sounding vaguely similar to log aggregation, the shipping of logs is a vastly different concept. Essentially, this involves the real-time copying of journal entries from a master data-centric system to one or more read-only replicas. Assuming state changes are fully captured as journal records, replaying those records allows the replicas to accurately mimic the state of the master, albeit with some lag.

Kafka’s optional ability to partition records within a topic to create independent, causally ordered sequences of events allows for replicas to operate in one of sequential or causal consistency models — depending on the chosen partitioning scheme. The various consistency models were briefly covered in Chapter 1: Event Streaming Fundamentals. Both consistency models are sufficient for creating read-only copies of the original data.

Log shipping is a key enabler for another related architectural pattern — **event sourcing**. Kafka will act as a durable event store, allowing any number of consumers to rebuild a point-in-time snapshot of their application state by replaying all records up to that point in time. Loss of state information in any of the downstream consumers can be recovered by replaying the events from the last stable checkpoint, thereby reducing the need to take frequent backups.

### SEDA pipelines

**Staged Event-Driven Architecture** (SEDA) is the application of pipelining to event-oriented data. Events flow unidirectionally through a series of processing stages linked by topics, each one performing a mapping operation before publishing a transformed event to the next topic. Intermediate stages simultaneously act as both consumers and producers, and may scale autonomously and independently of one another to match their unique load demands. By breaking a complex problem into stages, SEDA improves the modularity of the system.

A SEDA pipeline may combine fan-in and fan-out topologies. Stages may consume events from multiple topics simultaneously, performing the equivalent of an SQL JOIN on event streams. Stages can also publish to multiple topics, feeding several downstream pipelines.

As a pattern, SEDA is readily found in data warehousing, data lakes, reporting, analytics, and other Business Intelligence systems, and is often a crucial element of Big Data applications. SEDA can also be used in log aggregation; in fact, log aggregation is a narrow specialisation of SEDA.

### CEP

**Complex Event Processing** (CEP) extracts meaningful information and patterns in a stream of discrete events, or across a set of disjoint event streams. CEP processors tend to be stateful, as they must be able to efficiently recall prior events to identify patterns that might span a broad timeframe, ranging from milliseconds to days, depending on the context.

CEP is heavily employed in such applications as algorithmic stock trading, security threat analysis, real-time fraud detection, and control systems.

### Event-sourced CQRS

**Command-Query Responsibility Segregation** (CQRS) separates the actions that mutate state from the actions that query the state. Because mutations and queries typically exhibit contrasting runtime characteristics and require vastly different, often contradictory optimisation decisions, the separation of these concerns is conducive to building highly performant systems. The flip side is complexity — requiring multiple datastores and duplication of data — each datastore will maintain a unique projection of the master dataset, built from a dedicated data pipe. Kafka curbs some of the complexity inherent in CQRS architectures by acting as a common event-sourced ledger, using the concept of consumer groups to individually feed the different query-centric datastores.

This pattern is related to log shipping, and might appear identical at first glance. The differences are subtle. Log shipping is performed on low-level, internal representations of data, where both the replicas and the master datastore are coupled to, and share the same internal data structures. Put differently, log shipping is an internal mechanism that shuttles data within the confines of a single domain. In comparison, CQRS assumes disparate systems and spans domain boundaries. All parties are coupled to some canonical representation of events, which is versioned independently of the parties’ internal representations of those events.

This chapter has introduced the reader to Apache Kafka — the world’s most recognised and widely deployed event streaming platform. We looked at the history behind Kafka — how it started its journey and what it has become.

We also explored the various use cases that Kafka comfortably enables and supports. These are vast and varied, demonstrating Kafka’s overall flexibility and eagerness to cater to a diverse range of event streaming scenarios.

---

# Chapter 3: Architecture and Core Concepts

The first two chapters have furnished a cushy debut of the essential concepts of event streaming and have given the reader an introduction to Apache Kafka as the premier event streaming technology that is increasingly used to power organisations of all shapes and sizes — from green-sprout startups to multi-national juggernauts.

Now that the scene has been set, it is time to take a deeper look at how Kafka works, and more to the point, how one works with it.

## Architecture Overview

While the intention isn’t to indoctrinate the reader with the minutia of Kafka’s inner workings (for now), some appreciation of its design will go a long way in explaining the foundational concepts that will be covered shortly.

Kafka is a distributed system comprising several key components. At an outline level, these are:

- **Broker nodes:** Responsible for the bulk of I/O operations and durable persistence within the cluster.
- **ZooKeeper nodes:** Under the hood, Kafka needs a way of managing the overall controller status within the cluster. ZooKeeper fulfills this role, additionally acting as a consistent state repository that can be safely shared among the brokers.
- **Producers:** Client applications responsible for appending records to Kafka topics.
- **Consumers:** Client applications that read from topics.

### Broker nodes

Before we begin, it is worth noting that industry literature uses the terminology ‘Kafka Server’, ‘Kafka Broker’ and ‘Kafka Node’ interchangeably to refer to the same concept. The official documentation refers to all three, while the shell scripts for starting Kafka and ZooKeeper refer to both as ‘server’. This book favours the term ‘broker’ or, in some cases, the more elaborate ‘broker node’, avoiding the use of ‘server’ for its ambiguity.

So, what is a broker? If the reader comes from a background of messaging and middleware, the concept of a broker should resonate innately and intuitively. Otherwise, the reader is invited to consider Wikipedia’s definition:

> A broker is a person or firm who arranges transactions between a buyer and a seller for a commission when the deal is executed.

Sans the commission piece, the definition fits Kafka like a glove. We would, of course, substitute ‘buyer’ and ‘seller’ for ‘consumer’ and ‘producer’, respectively, but one point is clear — the broker acts as an intermediary, facilitating the interactions between two parties — adding value in between.

This might make one wonder: Why couldn’t the parties interact directly? A comprehensive answer would bore into the depths of computer science, specifically into the notion of coupling. We are not going to do this, to much relief; instead, the answer will be condensed to the following: the parties might not be aware of one another or they might not be jointly present at the same point in time. The latter places an additional demand on the broker: it must be stateful. In other words, it must persist the records emitted by the producer, so that they may be eventually delivered to the consumer when it is convenient to do so. The broker needs to be not only persistent, but also durable. By ‘durable’, it is implied that its persistence guarantees can be extended over a period of time and canvas scenarios that involve component failure.

> Discussions of brokers as a means of decoupling communicating parties may conjure images of message queues from the days of yore. Many Kafka purists would protest: Kafka is not a message queue, but an event streaming platform. While the argument holds on the whole, the underpinning objectives remain largely unchanged. Fundamentally, we still have a publishing party, a subscribing party, and an intermediary to facilitate their interaction. And we would ideally like the parties to remain minimally coupled.

A Kafka broker is a Java process that acts as part of a larger cluster, where the minimum size of the cluster is one. (Indeed, we often use a singleton cluster for testing.) A broker is one of the units of scalability in Kafka; by increasing the number of brokers, one can achieve improved I/O, availability, and durability characteristics. (There are other ways of scaling Kafka, as we shall soon discover.)

A broker fulfills its persistence obligations by hosting a set of append-only log files that comprise the partitions hosted by the cluster. A more thorough discussion on partitions is yet to come; it will suffice to say for now that partitions are elemental units of storage that one can address in Kafka. Each partition is mastered by exactly one broker — the partition leader. Partition data is replicated to a set of zero or more follower brokers. Collectively, the leader and the followers are referred to as replicas. Brokers share the load of leader and follower roles among themselves; a broker node may act as the leader for certain replicas, while being a follower for others. The roles may change — a follower replica may be promoted to leader status in the event of failure or as part of a manual rebalancing operation. The notion of replicas satisfies the durability guarantee; the more replicas in a cluster, the lower the likelihood of data loss due to an isolated replica failure.

Broker nodes are largely identical in every way; each node competes for the mastership of partition data on equal footing with its peers. Given the symmetric nature of the cluster, Kafka requires a mechanism for arbitrating the roles within the cluster and assigning partition leadership statuses among the broker nodes. Rather than making these decisions collectively, broker nodes follow a rudimentary chain-of-command. A single node is elected as the cluster controller which, in turn, directs all nodes (including itself) to assume specific roles. In other words, it is the controller’s responsibility for managing the states of partitions and replicas, and for performing administrative tasks like reassigning partitions among the broker nodes.

### ZooKeeper nodes

While the controller is entrusted with key administrative operations, the responsibility for electing a controller lies with another party — ZooKeeper. In fact, ZooKeeper is itself a cluster of cooperating processes called an ensemble. Every broker node will register its intent with ZooKeeper, but only one will be elected as the controller. ZooKeeper ensures that at most one broker node will be assigned the controller status, and should the controller node fail or leave the cluster, another broker node will promptly take its place.

A ZooKeeper ensemble also acts as a consistent and highly available configuration repository of sorts, maintaining cluster metadata, leader-follower states, quotas, user information, access control lists, and other housekeeping items. Owing to the underlying gossiping and consensus protocol of the ZooKeeper ensemble, the number of ZooKeeper nodes must be odd.

While ZooKeeper is bundled with Kafka for convenience, it is important to acknowledge that ZooKeeper is not an internal component of Kafka, but an open-source project in its own right.

### Producers

A Kafka producer is a client application that can act as a source of data in a Kafka cluster. A producer communicates with the cluster over a set of persistent TCP connections, with an individual connection established with each broker. Producers can publish records to one or more Kafka topics, and any number of producers can append records to the same topic. Generally speaking, only producers are allowed to append records to topics; a consumer cannot modify a topic in any way.

### Consumers

A consumer is a client application that acts as a data sink, subscribing to streams of records from one or more topics. Consumers are conceptually more complex than producers — they have to coordinate among themselves to balance the load of consuming records and track their progress through the stream.

## Total and partial order

A minor note before we proceed: there is some theory ahead that one must endure to become an effective purveyor of Kafka. The upcoming content may seem esoteric and somewhat detached from the subject matter at first; however, the reader is assured that it is most relevant. We will be as brief as possible.

Without exaggeration, Kafka’s entire event processing architecture is largely underpinned by the two primordial attributes of set theory: **partial order** and **total order**.

On the topic of set theory, what is a set? A set is a collection of distinct elements — objects that exist in their own right. For example, the numbers 2, 4, and 6 are distinct objects; when they are considered collectively, they form a set of size three, written `{2, 4, 6}`. Developed at the end of the 19th century, set theory is now a ubiquitous part of mathematics; it is also generally considered fundamental to the construction of distributed and concurrent systems.

A **totally ordered set** is one where every element has a well-defined ordering relationship with every other element in the set. Consider, for example, the range of natural numbers one to five. When sorted in increasing order, it forms the following sequence: `1, 2, 3, 4, 5`.

This is an ordered set, as every element has a well-defined predecessor-successor relationship with every other element. One can remove arbitrary values from this set and reinsert those values back into the set and arrive at the same sequence, no matter how many times this is attempted. Stated otherwise, there is only one permutation of elements that satisfies the ordering constraints.

Ordered sets exhibit the convenient property of transitivity. From the above example, we know that 2 must come after 1 and before 3. We also know that 3 must come before 4. Therefore, we can use the transitivity relation to deduce that 4 must come after 2.

The direct antithesis of a totally ordered set is an unordered set. For example, an offhand list of capital cities `{Sydney, New York, London}` is unordered. Without applying further constraints, one cannot reason whether Sydney should appear before or after New York. One can arbitrarily permute the elements to arrive at different sequences of cities without upsetting anyone.

Between the two extremes, we find a **partially ordered set**. Consider the set of natural numbers ordered by divisibility, such that a number must appear after its divisor. For the range of numbers two to twelve, one instantiation of a sequence that satisfies this partial ordering constraint might be `[2, 3, 5, 7, 11, 4, 6, 9, 10, 8, 12]`. But that is just one instance — there are several such sequences that are distinct, yet equivalent. Looking at the set, we can state that the numbers 4 and 6 must appear after 2, but there is no predecessor-successor relationship between 4 and 6 — they are mutually incomparable.

Multiple totally ordered sets can be contained in a single partially ordered set. For example, consider the Latin and Cyrillic alphabet sets `{A, B, C, ..., Z}` and `{А, Б, В, ..., Я}`, with their elements (letters) arranged in alphabetical order — forming two distinct, totally ordered sets. Their union would be a partially ordered set; it would still maintain the relative order within each alphabet, without imposing order across alphabets.

Like in a totally ordered set, the elements of partially ordered sets exhibit transitivity. In the example involving divisors, the number 2, appearing before 4, where 4 appears before 8 or 12, implies that both 8 and 12 must appear after 2.

Topping off this discussion is the term **‘causal order’**. This type of ordering was first brought up in Chapter 1: Event Streaming Fundamentals, as part of the discussion on the consistency of replicated state. Unlike the other flavours, causal order is not a carryover from 19th-century mathematics; it stems from the study of distributed systems. A notable challenge of constructing such systems is that messages sent between processes may arrive zero or more times at any point after they are sent. As a consequence, there is no agreeable notion of time among collaborating processes. If one process, such as a clock, sends a timestamped message to another process, there is no reliable way for a receiver to determine the time relative to the clock and synchronise the two processes. This is often cited as the sole reason that building distributed systems is hard.

In the absence of a global clock, the relative timing of a pair of events occurring in close succession may be indistinguishable to an outside observer; however, if the two events are causally related, it is possible to distinguish their order; in other words, they become comparable. Causal order is a semantic rendition of partial order, where two elements may be bound by a **happened-before** relationship. This is denoted by an arrow appearing between the two elements; for example, if `A → B`, then A is an event that must logically precede B. This further implies that A occurred before B in a chronological sense. Otherwise, if not `(A → B)`, A cannot have preceded B in a causal sense. The latter does not imply that A could not have physically occurred before B; one simply has no way of ascertaining this.

Causal relationships in distributed systems do not necessarily correspond to the more prevalent deductive ‘cause-and-effect’ style of logical reasoning. A causal relationship between a pair of events simply implies that one event precedes, rather than induces, the other. And it may be that the original events themselves are not comparable, but the recorded observations of these events are. These observations are events in their own right, and may also exhibit causality.

Consider, for example, two samples of temperature readings `R0` and `R1` taken at different sites. They are communicated to a remote receiver and recorded in the order of arrival, forming a causal relationship on the receiver. If the message from `R0` was received first, we could confidently state that `received(R0) → received(R1)`. This does not imply that `sent(R0) → sent(R1)`, and it most certainly does not imply that `R0` played any part in inducing `R1`.

In considering the connection between the terms ‘ordered’, ‘unordered’, ‘partially ordered’, ‘causally ordered’, and ‘totally ordered’, one can draw the following synopsis:

- A **partially ordered set** implies that not every pair of elements needs to be comparable.
- A **totally ordered set** is a special case of a partially ordered set, where there exists a well-defined order between every conceivable element pair.
- An **unordered set** is also a special case of a partially ordered set, where there is no pair of comparable elements.
- **Causal order** is a rendition of partial order, where each element represents an event, and some pairs of events have a happened-before relationship.
- On its own, the term ‘ordered set’ is ambiguous, suggesting that the elements of a set exhibit some order-inducing relationships. This term is generally avoided.

With partial and total order out of the way, we can proceed to a discussion of records, topics, and partitions. The link between the latter and set theory will shortly become apparent.

## Records

A record is the most elemental unit of persistence in Kafka. In the context of event-driven architecture, which is chiefly how one is meant to use Kafka, a record typically corresponds to some event of interest. It is characterised by the following attributes:

- **Key:** A record can be associated with an optional non-unique key, which acts as a kind of classifier — grouping related records on the basis of their key. The key is entirely free-form; anything that can be represented as an array of bytes can serve as a record key.
- **Value:** A value is effectively the informational payload of a record. The value is the most interesting part of a record in a business sense — it is the record’s value that ultimately describes the event. A value is optional, although it is rare to see a record with a null value. Without a value, a record is largely pointless; all other attributes play a supporting role in conveying the value.
- **Headers:** A set of free-form key-value pairs that can optionally annotate a record. Headers in Kafka are akin to their namesake in HTTP — they augment the main payload with additional metadata.
- **Partition number:** A zero-based index of the partition that the record appears in. A record must always be tied to exactly one partition; however, the partition need not be specified explicitly when the record is published.
- **Offset:** A 64-bit signed integer for locating a record within its encompassing partition. Records are stored sequentially; the offset represents a logical sequence number of the record.
- **Timestamp:** A millisecond-precise timestamp of the record. A timestamp may be set explicitly by the producer to an arbitrary value, or it may be automatically assigned by the broker when a record is appended to the log.

Newcomers to Kafka usually have no problems grasping the concept of a record and understanding its internals, with the possible exception of the key attribute. Because Kafka is often likened to a database (albeit one for storing events), a record’s key is often incorrectly associated with a database key. This warrants prompt clarification, so as to not cause confusion down the track. Kafka does have a primary key, but it is not the record key. A record’s equivalent of a ‘primary key’ is the composition of the record’s partition number and its offset. A record’s key is not unique, and therefore cannot possibly serve as the primary key. Furthermore, Kafka does not have the concept of a secondary index, and so the record key cannot be used to isolate a set of matching records.

Instead, it is best to think of a key as a kind of a pigeonhole into which related records are placed. Records maintain an association with their key over their entire lifetime. One cannot alter the key, or any aspect of the record for that matter, once the record has been published. It is unfortunate that keys are named as they are; a classifier (or a synonym thereof) would have been more appropriate.

The reason why the term ‘key’ was chosen is likely due to its association with hashing. Kafka producers use keys to map records to partitions — an action that involves hashing of the key bytes and applying the modulo operator. This carries a close resemblance to how keys are hashed to yield a bucket in a hash table.

The correspondence between Kafka records and observed events may not be direct; for example, an event might spawn multiple Kafka records, typically emitted in close succession. Those records may, in turn, be processed by a staged event-driven pipeline — spawning additional records in the course of processing. An overview of the staged event-driven architecture (SEDA) pattern was presented in Chapter 2: Introducing Apache Kafka.

Kafka is often used as a communication medium between fine-grained application services or across entire application domains. In saying that, the employment of Kafka as an internal note-taking or ledgering mechanism within a bounded context, is a perfectly valid use case. In fact, both Event Sourcing and CQRS patterns have seen strong adoption within the confines of single a domain, as well as across domains.

The recorded event might not have a real-life equivalent, even indirectly or circumstantially; there is no assumption or implication that Kafka is used solely as a registry of events. This statement may ruffle a few feathers or spark an all-out bellum sacrum; after all, Kafka is an event streaming platform — if not for recording events, what could it possibly be used for? Well, it might be used to replace a more traditional message broker. Much to the despise of Kafka purists (or delight, depending on one’s personal convictions), an increasingly-growing use for Kafka is to replace technologies such as RabbitMQ, ActiveMQ, AWS SQS and SNS, Google Cloud Pub/Sub, and so forth. Kafka renowned flexibility lets it comfortably deal with a broad range of messaging topologies and applications, some of which have little resemblance to classical event-driven architecture.

The generally accepted relationship between the terms ‘message’ and ‘event’ is such that a message encompasses a general class of asynchronous communiqués, while an event is a semantic specialisation of a message that communicates that some action of significance has occurred. Events have one logical owner — the producer (or publisher); they are immutable; they can be subscribed to and unsubscribed from. The term ‘event’ is often contrasted with another term — ‘command’ — a specialised message encompassing a directive issued from one party to another, requesting it to perform some action. The logical owner of a command is its sole recipient; it cannot be subscribed to or unsubscribed from.

Kafka documentation and client APIs mostly prefer the term ‘record’, where others might use ‘message’ or ‘event’. Kafka literature occasionally uses ‘message’ as a substitute for ‘record’, but this has been generally discouraged within the community, for the angst of confusing Kafka with the more traditional message-oriented middleware. In this book, the term ‘record’ is preferred, particularly when working in the context of event streaming. The term ‘event’ will generally be used to refer to an external action that triggered the publishing of the record, but may also be metonymically used to refer to the record itself, as it is often convenient to do so. Finally, this book may occasionally use the term ‘message’ when describing records in the context of a more traditional message broker, where the use of this term aids clarity.

## Partitions

A partition is a totally ordered, unbounded set of records. Published records are appended to the head-end of the encompassing partition. Where a record can be seen as an elemental unit of persistence, a partition is an elemental unit of record streaming.

Because records are totally ordered within their partition, any pair of records in the same partition is bound by a predecessor-successor relationship. This relationship is implicitly assigned by the producer application. For any given producer instance, records will be written in the order they were emitted by the application. By way of example, assume a pair of records P and Q destined for the same partition. If record P was published before Q, then P will precede Q in the partition. Furthermore, they will be read in the same order by all consumers; P will always be read before Q, for every possible consumer. This ordering guarantee is vital when implementing event-driven systems, more so than for peer-to-peer messaging or work queues; published records will generally correspond to or derive from real-life events, and preserving the timeline of these events is often essential.

Records published to one partition by the same producer are causally ordered. In other words, if P precedes Q, then P must have been observed before Q on the producer; the happened-before relationship is preserved — imparted from the producer onto the partition.

There is no recognised causal ordering across producers; if two (or more) producers emit records simultaneously for the same partition, those records may materialise in arbitrary order. The relative order will not depend on which producer application attempted to publish first, but rather, which record beat the other to the partition leader. That said, whatever the order, it will be consistent — observed uniformly across all consumers. Total order is still preserved, but some record pairs may only be related circumstantially.

Corollary to the above, in the absence of producer synchronisation, causal order can only be achieved when a single producer emits records to the same partition. All other combinations — involving multiple unrelated producers or different partitions — may result in a record stream that fails to depict causality. Whether or not this is an issue will depend largely on the application.

A record’s offset uniquely identifies it in the partition. The offset acts as a primary key, allowing for fast, O(1) lookups. The offset is a strictly monotonically-increasing integer in a sparse address space, meaning that each successive offset is always higher than its predecessor, and there may be varying gaps between neighbouring offsets. Gaps might legitimately appear if compaction is enabled or as a result of transactions; we don’t need to delve into the details at this stage, suffice it to say that offsets need not be contiguous.

Given a record, an application shouldn’t attempt to literally interpret its offset or guess what the next offset might be. It may, however, actively exploit the properties of total order and transitivity to infer the relative order of any record pair based on their offsets, sort the records by their offset, and so forth.

The **beginning offset**, also called the low-water mark, is the first record that will be presented to a prospective consumer. Due to Kafka’s bounded retention, this is not necessarily the first record that was published. Records may be pruned on the basis of time and/or partition size. When this occurs, the low-water mark will appear to advance, and records earlier than the low-water mark will be truncated.

Conversely, the **high-water mark** is the offset immediately following the last successfully replicated record. Consumers are only allowed to read up to the high-water mark. This prevents a consumer from reading unreplicated data that may be lost in the event of leader failure. The equivalent term for a high-water mark is the **end offset**.

The statement above is a minor simplification. The end offset corresponds to the high-water mark for non-transactional consumers. Where a more strict isolation mode has been selected on the consumer, the end offset may trail the high-water mark. Transactional messaging is an advanced topic, covered in Chapter 18: Transactions.

The end offset should not be confused with the internal term ‘log end offset’, which is the offset immediately following that of the last written record. The ‘log end offset’ will be assigned to the next record that will be published. When the follower replicas lag behind the leader, the ‘log end offset’ will be greater than the high-water mark. When replication eventually catches up, the high-water mark will align with the ‘log end offset’.

Subtracting the low-water mark from the high-water mark will yield the upper bound on the number of securely persisted records in the partition. The actual number may be slightly less, as the offsets are not guaranteed to be contiguous. The total number of records may be fewer or greater, as the high-water mark does not reflect the number of unreplicated records.

## Topics

So, a partition is an unbounded sequence of records, an open ledger, a continuum of events — each definition as good as the next. Along with a record, a partition is an elemental building block of an event streaming platform. But a partition is too basic to be used effectively on its own.

A **topic** is a logical aggregation of partitions. It comprises one or more partitions, and a partition must be a part of exactly one topic. Topics are fundamental to Kafka, allowing for both parallelism and load balancing.

Earlier, it was said that partitions exhibit total order. Taking a set-theoretic perspective, a topic is just a union of the individual underlying sets; since partitions within a topic are mutually independent, the topic is said to exhibit **partial order**. In simple terms, this means that certain records may be ordered in relation to one another, while being unordered with respect to certain other records. A Kafka topic, and specifically its use of partial order, enables us to process records in parallel where we can, while maintaining order where we must. The concept of consumer parallelism will be explored shortly; for the time being, the focus will remain on the producer ecosystem.

Since Kafka is an event streaming platform, it may be more instructive to think of a topic and its partitions as a wide stream, comprising multiple parallel substreams. Events within a substream may be related objectively, insofar as one event must precede some other event. In other words, a causal relationship is in place. (The events are not required to be causally related to share a substream; the reason that will be touched on later.) Events across substreams are related subjectively — they might refer to a similar class of observations and it may be advantageous to encompass them within the same stream.

Occasionally, this book will use the term ‘stream’ as a substitute for ‘topic’; when referring to events, the use of the term ‘stream’ is often more natural and intuitive.

Precisely how records are partitioned is left to the discretion of the producer. A producer application may explicitly assign a partition number when publishing a record, although this approach is rarely used. A much more common approach is for the client application to deal exclusively with record keys and values, and have the producer library automatically select a partition on the basis of a record’s key. A producer will digest the byte content of the key using a hash function (Kafka uses murmur2 for this purpose). The highest-order bit of the hash value is masked off to force it to a positive integer, before taking the result, modulo the number of partitions, to arrive at the final partition number.

While this partitioning scheme is deterministic, it is not consistent. Two records with the same key hashed at different points in time will correspond to an identical partition number if and only if the number of partitions has not changed in that time. Increasing the number of partitions in a topic (Kafka does not support non-destructive downsizing) results in the two records occupying potentially different partitions — leading to a breakdown of any prior order. There are many gotchas, such as this one, in Kafka; they will be called out as such from time to time.

Records sharing the same hash are guaranteed to occupy the same partition. Assuming a topic with multiple partitions, records with a different key will likely end up in different partitions. However, due to hash collisions, records with different hashes may also end up in the same partition. Such is the nature of hashing; if the reader appreciates how a hash table works, this is no different. It was previously stated that records in the same partition may be causally related, but do not have to be. The reason is specifically to do with hashing; when there are more causally related record groupings than there are partitions in a topic, there will invariably be some partitions that contain multiple unrelated sets of records. In mathematics, this is referred to as the Dirichlet’s Drawer Principle or the Pigeonhole Principle. In fact, due to the imperfect space distribution of hash functions, unrelated records will likely be grouped in the same partition even if there are more partitions than distinct keys.

Producers rarely care which specific partition the records will map to, only that related records end up in the same partition, and that their order is preserved. Similarly, consumers are largely indifferent to their assigned partitions, so long that they receive the records in the same order as they were published, where those records are causally bound.

## Consumer groups and load balancing

So far we have learned that producers emit records to a topic; these records are organised into neatly ordered partitions. Kafka’s producer-topic-consumer topology adheres to a flexible and highly generalised multipoint-to-multipoint model, meaning that there may be any number of producers and consumers simultaneously interacting with a topic. Depending on the actual solution context, topologies may also be point-to-multipoint, multipoint-to-point, and point-to-point. Kafka does not impose the sorts of limits that one is used to seeing from the more ‘orthodox’ messaging middleware.

It’s about time we looked at how records are consumed.

A consumer is a process or thread that attaches to a Kafka cluster via a client library. A consumer generally, but not necessarily, operates as part of an encompassing **consumer group**. Consumer groups are effectively a load-balancing mechanism within Kafka — distributing partition assignments approximately evenly among the individual consumer instances within the group. When the first consumer in a group subscribes to the topic, it will receive all partitions in that topic. When a second consumer subsequently joins, it will get approximately half of the partitions, relieving the first consumer of half of its prior load. The process runs in reverse when consumers leave (by disconnecting or timing out) — the remaining consumers will absorb a greater number of partitions.

This book will occasionally use the term ‘subscriber’ to collectively refer to all consumer instances in a consumer group, as a single, logical entity. When referring to event streams, the notion of a subscriber is sometimes more intuitive and helps distinguish between those consumers who may not be a part of a consumer group at all. Those sorts of consumers will be discussed later.

So, a consumer siphons records from a topic, pulling from the share of partitions that have been assigned to it by Kafka, alongside the other consumers in its group. As far as load-balancing goes, this is nothing out of the ordinary. But here’s the kicker — consuming a record does not remove it from the topic. This might seem contradictory at first, especially if one associates the act of consuming with depletion. (If anything, a consumer should have been called a ‘reader’, but let’s not dwell on the choice of terminology.) The simple fact is, consumers have absolutely no impact on the topic and its partitions; a topic is an append-only log that may only be mutated by the producer, or by Kafka itself as part of its housekeeping chores. Consumers are ‘cheap’, so to speak — you can have a fair number of them tail the logs without stressing the cluster. This is a yet another point of distinction between an event stream and a traditional message queue, and it’s a crucial one.

A consumer internally maintains an offset that points to the next record in a partition, advancing the offset for every successive read. In fact, a consumer maintains a vector of such offsets — one for each assigned partition. When a consumer first subscribes to a topic, whereby no offsets have been registered for the encompassing consumer group, it may elect to start at either the head-end or the tail-end of the topic. Thereafter, the consumer will acquire an offset vector and will advance the offsets internally, in line with the consumption of records.

In Kafka terminology, the ‘head’ of a partition corresponds to the location of the end offsets, while the ‘tail’ of the partition is the side closest to the beginning offsets. This might sound confusing if Kafka is perceived as a queue of sorts, where the head-end of a queue canonically corresponds to the side which has the oldest elements. In Kafka, the oldest elements are at the tail-end.

Since consumers across different consumer groups do not interfere, there may be any number of them reading concurrently from the same topic. Consumers run at their own pace; a slow or backlogged consumer has no impact on its peers.

To illustrate this concept, consider a contrived scenario involving a topic with two partitions. Two consumer groups — A and B — are subscribed to the topic. Each group has three consumer instances, named A0, A1, A2, B0, B1, and B2.

As part of fulfilling the subscriptions, Kafka will allocate partitions among the members of each group. In turn, each group will acquire and maintain a dedicated set of offsets that reflect the overall progress of the group through the topic.

Upon careful inspection, the reader will notice that something is missing. Two things, in fact: consumers A2 and B0 aren’t there. That is because Kafka ensures that a partition may only be assigned to at most one consumer within its consumer group. (It is said ‘at most’ to cover the case when all consumers are offline.) Because there are three consumers in each group, but only two partitions, one consumer will remain idle — waiting for another consumer in its respective group to depart before being assigned a partition. In this manner, consumer groups are not only a load-balancing mechanism, but also a fence-like exclusion control, used to build highly performant pipelines without sacrificing safety, particularly when there is a requirement that a record may only be handled by one thread or process at any given time.

Consumer groups also ensure availability, satisfying the **liveness** property of a distributed consumer ecosystem. By periodically reading records from a topic, the consumer implicitly signals to the cluster that it is in a ‘healthy’ state, thereby extending the lease over its partition assignment. Should the consumer fail to read again within the allowable deadline, it will be deemed faulty and its partitions will be reassigned — apportioned among the remaining ‘healthy’ consumers within its group.

A thorough discussion of the safety and liveness properties of Kafka will be deferred until Chapter 15: Group Membership and Partition Assignment. For the time being, the reader is asked to accept an abridged definition: Liveness is a property that requires a system to eventually make progress, completing all assigned work. Safety is a property that requires the system to respect all its key invariants, at all times.

To employ a transportation analogy, a topic is like a highway, while a partition is a lane. A record is the equivalent of a car, and its occupants correspond to the record’s value. Several cars can safely travel on the same highway, providing they keep to their lane. Cars sharing the same line ride in a sequence, forming an orderly queue. Now suppose each lane leads to an off-ramp, diverting its traffic to some location. If one off-ramp gets banked up, other off-ramps may still flow smoothly.

It is precisely this highway-lane metaphor that Kafka exploits to achieve its trademark end-to-end throughput, easily reaching millions of records per second on commodity hardware. When creating a topic, one can set the partition count — the number of lanes, if you will. The partitions are divided approximately evenly among the individual consumers in a consumer group, with a guarantee that no partition will be assigned to two (or more) consumers at the same time, providing that these consumers are part of the same consumer group. Referring to our analogy, a car will never end up in two off-ramps simultaneously; however, two lanes might conceivably merge to the same off-ramp.

### Committing offsets

It has already been said that consumers maintain an internal state with respect to their partition offsets. At some point, that state must be shared with Kafka, so that when a partition is reassigned, the new consumer can resume processing from where the outgoing consumer left off. Similarly, if the consumers were to disconnect, upon reconnection they would ideally skip over any records that have already been processed.

Persisting the consumer state back to the Kafka cluster is called **committing an offset**. Typically, a consumer will read a record (or a batch of records) and commit the offset of the last record plus one. If a new consumer takes over the topic, it will commence processing from the last committed offset — hence the plus-one step is essential. (Otherwise, the last processed record would be handled a second time.)

> Curious fact: Kafka employs a recursive approach to managing committed offsets, elegantly utilising itself to persist and track offsets. When an offset is committed, the group coordinator will publish a binary record on the internal `__consumer_offsets` topic. The contents of this topic are compacted in the background, creating an efficient event store that progressively reduces to only the last known commit points for any given consumer group.

Controlling the point when an offset is committed provides a great deal of flexibility around delivery guarantees, further highlighting Kafka’s adaptability towards various messaging scenarios. The term ‘delivery’ assumes not just reading a record, but the full processing cycle, complete with any side-effects. (For example, updating a database, or invoking a service.) One can shift from an at-most-once to an at-least-once delivery model by simply moving the commit operation from a point before the processing of a record is commenced, to a point sometime after the processing is complete. With this model, should the consumer fail midway through processing a record, it will be re-read following partition reassignment.

By default, a Kafka consumer will automatically commit offsets at an interval of at least every five seconds. The interval will automatically be extended in the presence of in-flight records — the records that are still being processed on the consumer. The lower bound on this interval can be controlled by the `auto.commit.interval.ms` configuration property, which is discussed in Chapter 10: Client Configuration. An implication of the offset auto-commit feature is that it extends the window of uncommitted offsets beyond the set of in-flight records; in other words, the consumer might finish processing a batch of records without necessarily committing the offsets. If the consumer’s partitions are then reassigned, the new consumer will end up processing the same batch a second time. To constrain the window of uncommitted records, one needs to take offset committing into their own hands. This can be done by setting the `enable.auto.commit` client property to `false`.

Getting offset commits right can be tricky, and routinely catches out beginners. A committed offset implies that the record one below that offset and all prior records have been dealt with by the consumer. When designing at-least-once applications, an offset should only be committed when the application has dealt with the record in question, and all records before it. In other words, the record has been processed to the point that any actions that would have resulted from the record have been carried out and finalised. This may include calling other APIs, updating a database, committing transactions, persisting the record’s payload, or publishing more records. Stated otherwise, if the consumer were to fail after committing the record, then not ever seeing this record again must not be detrimental to its correctness.

In the at-least-once scenario, a typical consumer implementation will commit its offsets linearly, in tandem with the processing of a record batch. That is, read a record batch from a topic, process the individual records, commit the outstanding offsets, read the next batch, and so on. This is called a **poll-process loop**.

The poll-process loop is a somewhat simplified take on reality. We will not go into the details of how records are fetched from Kafka when `KafkaConsumer.poll()` is called; a more thorough description is presented in Chapter 7: Serialization. We will remark on one optimisation: a consumer does not always fetch records directly from the cluster; it employs a prefetch buffer to pipeline this process.

A common tactic is to process a batch of records concurrently (where this makes sense), using a thread pool, and only confirm the last record when the entire batch is done. The commit process in Kafka is very efficient, the client library will send commit requests asynchronously to the cluster using an in-memory queue, without blocking the consumer. The client application can register an optional callback, notifying it when the commit has been acknowledged by the cluster. And there is also a blocking variant available should the client application prefer it.

## Free consumers

The association of a consumer with a consumer group is an optional one, indicated by the presence of a `group.id` consumer property. If unset, a **free consumer** is presumed. Free consumers do not subscribe to a topic; instead, the consuming application is responsible for manually assigning a set of topic-partitions to itself, individually specifying the starting offset for each topic-partition pair. Free consumers do not commit their offsets to Kafka; it is up to the application to track the progress of such consumers and persist their state as appropriate, using a datastore of their choosing. The concepts of automatic partition assignment, rebalancing, offset persistence, partition exclusivity, consumer heartbeating and failure detection (safety and liveness, in other words), and other so-called ‘niceties’ accorded to consumer groups cease to exist in this mode.

> The use of the nominal expression ‘free consumer’ to denote a consumer without an encompassing group is a coined term. It is not part of the standard Kafka nomenclature; indeed, there is no widespread terminology that marks this form of consumer.

Free consumers are not observed in the wild as often as their grouped counterparts. There are predominantly two use cases where a free consumer is an appropriate choice. One such case is when an application genuinely requires full control of the partition assignment scheme, likely utilising a dedicated datastore to track consumer offsets. This is very rare. Needless to say, it is also difficult to implement correctly, given the multitude of scenarios one must account for. It is mentioned here only for completeness.

The more commonly seen use case is when a stateless or ephemeral consumer needs to monitor a topic. For example, an application might tail a topic to identify specific records, or just as a monitoring or debugging aid. One might only care about records that were published when the stateless consumer was online, so concerns such as persisting offsets and resuming from the last processed record become largely irrelevant. A good example of where this is used routinely is the Kafdrop tool, which we will explore in one of the upcoming chapters. When the user clicks on a topic to view the records, Kafdrop creates a free consumer and assigns the requested partition to it, reading the records from the supplied offsets. Navigating to a different topic or partition will reset the consumer, discarding any prior state.

One scenario that benefits from free consumers is the implementation of the sync-over-async pattern using Kafka. For example, a producer might issue a command-style request (or query) to a downstream consumer and expect a response on the same or different topic. The initiating producer might be operating in a synchronous context; for example, it might be responding to a synchronous request of its own, and so it has no choice but to wait for the downstream response before proceeding. To complicate matters, there might be multiple such initiators in operation, and it is essential that the response is processed by the same initiator that issued the original request.

The sync-over-async scenario is a special case of the stateless consumer scenario presented above. The initiator starts by assigning itself all partitions of the response topic and resetting the offsets to the high-water mark. It then publishes the request command with a unique identifier that will be echoed in the response. (Typically, this is a UUID.) The downstream consumer will eventually process the message and publish its response. Meanwhile, the initiator will poll the topic for responses, filtering by ID. Eventually, either the response will arrive within a set deadline, or the initiator will time out. Either way, the assignment of the partitions to the initiator is temporary, and no state is preserved between successive assignments.

## Summary of core concepts

The key takeaways are:

- A cluster hosts multiple topics, each having an assigned leader and zero or more follower replicas.
- Topics are subdivided into partitions, with each partition forming an independent, totally-ordered sequence within a wider, partially-ordered stream.
- Multiple producers are able to publish to a topic, picking a partition at will. The partition may be selected directly — by specifying a partition number, or indirectly — by way of a record key, which deterministically hashes to a partition number.
- Partitions in a topic can be load-balanced across a population of consumers in a consumer group, allocating partitions approximately evenly among the members of that group.
- A consumer in a group is not guaranteed a partition assignment. Where the group’s population outnumbers the partitions, some consumers will remain idle until this balance equalises or tips in favour of the other side.
- A consumer will commit the offset of a record when it is done processing it. The commits are directed to a consumer coordinator, which will end up written to an internal `__consumer_offsets` topic. The offset of the record is incremented by one before committing, to prevent unnecessary replay.
- Partitions may be manually assigned to free consumers. If necessary, an entire topic may be assigned to a single free consumer — this is done by individually assigning all partitions.

Event streaming platforms are a highly effective building block in the construction of modular, loosely-coupled, event-driven applications. Within the world of event streaming, Kafka has solidified its position as the go-to open-source solution that is both flexible and highly performant.

Concurrency and parallelism are at the heart of Kafka’s architecture, forming partially-ordered event streams that can be load-balanced across a scalable consumer ecosystem. A simple reconfiguration of consumers and their encompassing groups can bring about vastly different event distribution and processing semantics; shifting the offset commit point can invert the delivery guarantee from an at-most-once to an at-least-once model.

The consumer group is a somewhat understated concept that is pivotal to the versatility of an event streaming platform. By simply varying the affinity of consumers with their groups, one can arrive at vastly different distribution topologies — from a topic-like, pub-sub behaviour to an MQ-style, point-to-point model. Because records are never truly consumed (the advancing offset only creates the illusion of consumption), one can concurrently superimpose disparate distribution topologies over a single event stream.

---

# Chapter 4: Installation

Previous chapters have given us a reasonably grounded understanding of what Kafka is and isn’t, where it is used, and how it dovetails into the rest of the software landscape. So far we have just been circling the shore; time to dive in for a deeper look. Before we can get much further, we need a running Kafka setup.

This will require us to install several things:

1. **Kafka and ZooKeeper.** Recall from our earlier coverage of the Kafka architecture, a functioning setup requires both Kafka and ZooKeeper nodes. These are plain Java applications with no other requirements or dependencies, and can run on any operating system and any hardware supported by Java.
2. **Kafdrop.** While this isn’t strictly required to operate Kafka, Kafdrop is the most widely-used web-based tool for working with Kafka, and would be widely considered as an essential item of your Kafka toolkit.
3. **A Java Development Kit (JDK).** Kafka is part-written in Scala and part in Java, and requires Java version 8 or newer to run. Kafdrop is somewhat more modern, requiring Java 11.

If you haven’t done so already, install a copy of the JDK. Version 11 or newer will do. The rest of the chapter assumes that you have a JDK installed.

> The requirement for JDK 11 is to accommodate Kafdrop. If you are installing Kafka on its own, JDK 8 will suffice. However, Java 11 is still recommended over Java 8 as it is the current Long-Term Support (LTS) version. The last free public updates of JDK 8 ended in January 2019. Free updates for JDK 11 continued through to September 2021, by which point the world would have transitioned to JDK 17 LTS. (LTS releases are publicly supported for three years.)

## Installing Kafka and ZooKeeper

There are at least four avenues at one’s disposal for installing Kafka and ZooKeeper:

1. Run Kafka and ZooKeeper using Docker.
2. Install Kafka and ZooKeeper using a package manager, such as DNF (formerly known as YUM) for Red Hat, CentOS and Fedora Linux distributions, APT for Debian and Ubuntu, and Homebrew for macOS. There are many others.
3. Clone the Kafka and ZooKeeper repositories and build from source code.
4. Download and unpack the official Kafka distribution from [kafka.apache.org/downloads](https://kafka.apache.org/downloads), which comes bundled with ZooKeeper.

Let’s briefly touch upon each of these options. It might sound like overkill at first — we could just pick the easiest and get cracking — but we’re in it for the long haul. And sometimes the easiest approach isn’t the right one.

Docker is an excellent all-round approach for getting started with Kafka, developing against a local Kafka broker, and even running Kafka in a production configuration. And best of all, a Docker image will come with an appropriate version of the JDK.

There is one drawback, however. Kafka in Docker is notoriously difficult to configure, as everything is baked in and not designed for change. (That’s not to say it can’t be done.) We won’t go down the Docker path for now, just because we will be making lots of changes to the broker configuration at various points along our journey, and ideally, we would want this process to be as simple and painless as possible.

Installing Kafka and ZooKeeper from a package is convenient too. And while it doesn’t come bundled with a JDK, a package will typically declare the JDK as a dependency, which the package manager will attempt to resolve at the point of installation (or when updating the package). Still, this approach isn’t without its drawbacks: there may be other applications installed on the target machine that might require a different version of the JDK, which would warrant further configuration of Kafka and ZooKeeper to wire it up to the correct JDK.

Another drawback to the ‘packaged Kafka’ approach is that the installation path and the layout of the files will vary depending on the chosen package manager. For example, on macOS, Homebrew installs Kafka into `/usr/local/etc`, `/usr/local/bin`, and `/usr/local/var/lib/`. On the other hand, YUM will install it under `/bin`, `/opt`, and `/var/lib`. This makes it tremendously difficult to write about and include worked examples that work consistently for all readers. Rather than focusing on the subject matter, this book would have been polluted with excerpts and call-outs targeting different operating systems and package managers.

The third option is building from source code. It might sound a bit extreme for someone who has just opened a book on Kafka. Nonetheless, it is a valid option and the only option if you happen to be a contributor. Understandably, we won’t delve into it much deeper, and blissfully pretend it doesn’t exist — at least in the universe bound by this book.

The final option, and the one we will inevitably proceed with, is to download the latest version of the official Kafka tarball from [kafka.apache.org/downloads](https://kafka.apache.org/downloads). There might be several options — pick the one in ‘Binary downloads’ that targets the latest version of Scala.

Copy the downloaded `.tgz` file into a directory of your choice, and unpack with:

```bash
tar zxf kafka_2.13-2.4.0.tgz
```

Replacing the filename as appropriate. (In this example, the downloaded version is 2.4.0, but your version will likely be newer.) The files will be unpacked to a subdirectory named `kafka_2.13-2.4.0`. We will refer to this directory as the **Kafka home directory**.

When referring to the home directory from a command-line example, we’ll use the constant `$KAFKA_HOME`. You have the choice of either manually substituting `$KAFKA_HOME` for the installation directory, or assigning the installation path to the `KAFKA_HOME` environment variable, as shown in the example below.

```bash
export KAFKA_HOME=/Users/me/opt/kafka_2.13-2.4.0
```

> You would need to run `export KAFKA_HOME...` at the beginning of every terminal session. Alternatively, you can append the `export...` command to your shell’s startup file. If you are using Bash, this is typically `~/.bashrc` or `~/.bash_profile`.

Take a moment to look around the home directory. You’ll see several subdirectories, chief among them being `bin`, `libs`, and `config`.

The `bin` directory contains the scripts to start and stop Kafka and ZooKeeper, as well as various CLI (command-line interface) utilities for working with Kafka. Also, `bin` contains a `windows` subdirectory, which contains the equivalent scripts for Microsoft Windows.

The `config` directory is another important one. It contains `.properties` files that are used to configure the various components that make up the Kafka ecosystem.

The `libs` directory contains the Kafka binary distribution, as well as its direct and transitive dependencies. You’ll never need to modify the contents of this directory.

## Launching Kafka and ZooKeeper

Now that the applications have been installed, we can start them. The first cab off the rank will be ZooKeeper, as it is a runtime requirement for Kafka. Run the following command in a terminal:

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh \
  $KAFKA_HOME/config/zookeeper.properties
```

This will launch ZooKeeper in foreground mode. You should see a bunch of messages logged to the console, signifying the starting of ZooKeeper. Among them, you might spot one warning message:

```text
[2019-12-25 13:30:36,951] WARN Either no config or no quorum defined in config, running in standalone mode (org.apache.zookeeper.server.quorum.QuorumPeerMain)
```

All this is saying is that we started ZooKeeper in standalone mode, without configuring a quorum. When running ZooKeeper locally, availability is rarely a concern, and a standalone (ensemble of one member node) configuration is sufficient.

Recall from a prior discussion on the Kafka architecture, it was stated that ZooKeeper acts as an arbiter — electing a sole controller among the available Kafka broker nodes. Internally, ZooKeeper employs an atomic broadcast protocol to agree on and subsequently maintain a consistent view of the cluster state throughout the ZooKeeper ensemble. This protocol operates on the concept of a majority vote, also known as quorum, which in turn, requires an odd number of participating ZooKeeper nodes. When running in a production environment, ensure that at least three nodes are deployed in a manner that no pair of nodes may be impacted by the same contingency. Ideally, ZooKeeper nodes should be deployed in geographically separate data centres.

Now that ZooKeeper is running, we can start Kafka. Run the following in a new terminal window:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh \
  $KAFKA_HOME/config/server.properties
```

Kafka’s logs are a bit more verbose than ZooKeeper’s. The first really useful part of the log relates to the ZooKeeper connection. Specifically, which ZooKeeper instance(s) Kafka is trying to connect to, and the status of the connection.

Among the logs we can also find the complete Kafka broker configuration. This is actually more useful than one might initially imagine. The Kafka broker configuration is defined in `$KAFKA_HOME/config/server.properties`, but the file is relatively small and initially contains mostly commented-out entries. This means that most settings are assigned their default values. Rather than consulting the official documentation to determine what the defaults might be and whether or not they are actually overridden in your configuration, you need only look at the broker logs. This is particularly useful when you need to debug the configuration.

The next useful bit of information is emitted by the socket listener:

```text
[2019-12-25 14:02:22,587] INFO Awaiting socket connections on 0.0.0.0:9092. (kafka.network.Acceptor)
```

This tells us that Kafka is listening for inbound connections on port `9092`, and is bound to all network interfaces (indicated by the IP meta-address `0.0.0.0`). This is corroborated by the deprecated property `port`, which defaults to 9092. There is a much more sophisticated mechanism for configuring listeners, which we will examine in one of the following chapters. For now, a `0.0.0.0:9092` listener will suffice.

Believe it or not, the most useful information one can get out of Kafka’s logs is actually the version number. Admittedly, it sounds somewhat banal, but how many times have you stared helplessly at the screen wondering why a piece of software that was just upgraded to the latest version still has the same bug that the authors have sworn they had fixed? Invariably, it is always some simple mistake — a symlink to the wrong binary, a typo in the path, a wrong value in an environment variable, or some other moth-eaten stuff-up along those lines. Printing the application version number in the logs is a simple way of eradicating these classes of errors.

## Running in the background

When launching ZooKeeper or Kafka, you have the option of running it as a daemon by passing it the `-daemon` flag. In simple terms, this means launching ZooKeeper in the background, without holding up the terminal. Kill the existing ZooKeeper process by pressing `CTRL+C`, and try the following:

```bash
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon \
  $KAFKA_HOME/config/zookeeper.properties
```

That’s all well and good, but where have the logs gone? When launched as a daemon, the standard output of the ZooKeeper process is piped to `$KAFKA_HOME/logs/zookeeper.out`. We can tail the logs by running:

```bash
tail -f $KAFKA_HOME/logs/zookeeper.out
```

To stop a daemon ZooKeeper process, run `$KAFKA_HOME/bin/zookeeper-server-stop.sh`. This will stop the background process if there is one running. If not, it will respond with `No zookeeper server to stop`.

ZooKeeper and Kafka shell scripts are essentially mirror images of each other. To launch Kafka as a daemon, run:

```bash
$KAFKA_HOME/bin/kafka-server-start.sh -daemon \
  $KAFKA_HOME/config/server.properties
```

Kafka standard output logs are written to `$KAFKA_HOME/logs/kafkaServer.out`. To stop a daemon Kafka process, run `$KAFKA_HOME/bin/kafka-server-stop.sh`.

## Installing Kafdrop

Next on our list is Kafdrop. It’s a Java application with no dependencies, and the avenues for installing it are mostly similar to Kafka, except it does not offer package-based installation. In practice, Docker largely obviates the need for packages, and the sheer number of Kafdrop Docker pulls (over a million at the time of writing) is a testament to that.

As practical as a Docker image may be, we are going to ditch this option for now. Because we are running Kafka on localhost, Docker will struggle to connect to our broker, as Docker containers are normally unaware of processes running on the host machine. There is a way to change this but it is not portable across Linux and macOS, and will also require changes to the Kafka broker configuration — something we are not yet prepared to do. We will revisit Docker later. For now, we will go with the official Kafdrop binary distribution.

Kafdrop binaries are hosted on Bintray, with a download link embedded in each release on GitHub. Open the releases page: [github.com/obsidiandynamics/kafdrop/releases](https://github.com/obsidiandynamics/kafdrop/releases) and pick the latest from the list. Alternatively, you can navigate straight to the latest Kafdrop release by following this shortcut: [github.com/obsidiandynamics/kafdrop/releases/latest](https://github.com/obsidiandynamics/kafdrop/releases/latest).

Clicking on the link will download a `.jar` file. Save it in a directory of your choice and run it as shown in the example below, replacing the filename as appropriate.

```bash
java -jar kafdrop-3.18.0.jar --kafka.brokerConnect=localhost:9092
```

Once started, you can open Kafdrop in your browser by navigating to [localhost:9000](http://localhost:9000). You’ll be presented with a Kafdrop cluster overview screen, showing our fresh, single-node Kafka cluster.

There are a few things of interest here. On the top-right corner, you should see the Kafdrop release version and buildstamp. This can be very useful if you are connecting to a remote Kafdrop instance, and don’t have the logs that disclose which version of Kafdrop is running.

The next section provides a summary of the cluster. Note the ‘Bootstrap servers’ field: it mirrors the `--kafka.brokerConnect` command-line argument, telling us how Kafdrop has been configured to discover the Kafka nodes. Bootstrapping and broker discovery is a whole topic on its own, which we are going to gloss over for now. For the time being, and unless stated otherwise, assume that the ‘bootstrap servers’ list is `localhost:9092`. We will revisit this topic in Chapter 8: Bootstrapping and Advertised Listeners.

The ‘Brokers’ section enumerates over the individual brokers in the cluster. We are running a single-broker setup just now, so seeing a one-line table entry should come as no surprise. Naturally, being the only broker in the cluster, it will have been assigned the controller role.

The ‘Topics’ section is empty, as we haven’t created any topics yet. Nor do we have any ACLs defined. This is all yet to come.

Switch back to the shell running Kafdrop. Looking over the standard output logs we can spot the version number. Keep looking and you’ll notice the port that Kafdrop is listening on and the context path. This is all configuration, and it’s something that may need to change between environments.

```text
2019-12-25 19:08:49.465 INFO 82515 [main] k.s.BuildInfo: Kafdrop version: 3.18.0, build time: 2019-12-02T08:36:13.356Z
...
(some logs omitted)
...
2019-12-25 19:08:50.752 INFO 82515 [main] o.s.b.w.e.u.UndertowServletWebServer: Undertow started on port(s) 9000 (http) with context path ''
```

We have learned about the various ways one can obtain and install a Kafka and ZooKeeper bundle. We have also started and stopped a basic ZooKeeper and Kafka setup, learned about foreground and daemon modes, and surveyed the logs for useful information. Finally, we installed Kafdrop and took a brief look around. The scene is now set; we just need to make use of it all somehow.

---

# Chapter 5: Getting Started

With the theoretical foundations nailed, and a fresh installation of Kafka standing by, it is time to roll up our sleeves for a more practical approach to learning Kafka.

This chapter will focus on the two fundamental operations: publishing records to Kafka topics and subsequently consuming them. We are going to explore the various mechanisms for interacting with the broker and also for exploring the contents of topics and partitions.

## Publishing and consuming using the CLI

When discussing producers and consumers, the first thing that might spring to mind is a set of bespoke applications that someone — an individual, or more likely, a team of developers — will build and maintain as part of operating a broader event-streaming system. But one does not need a fully-fledged application to publish to or consume from a Kafka topic — this task can be accomplished using the set of CLI (command-line interface) tools that are shipped with Kafka, located in the `$KAFKA_HOME/bin` directory.

### Creating a topic

Let’s get started then. The first thing is to create a topic, which can be accomplished using the `kafka-topics.sh` tool:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --partitions 3 --replication-factor 1 \
  --topic getting-started
```

Observe, although the parameter `--bootstrap-server` is named in singular form, the `kafka-topics.sh` tool will, rather unexpectedly, accept a comma-separated list of brokers. We have specified `localhost:9092` as the bootstrap server, because that is where our test cluster is currently running. If you are using a remote Kafka broker or a managed Kafka service, you will have been provided with an alternate list of broker addresses.

> The packaged CLI utilities are not the most intuitive of tools that one can use with Kafka; in fact, they are widely regarded as being awkward to use and barely adequate in functionality. Most Kafka practitioners have long abandoned the out-of-the-box utilities in favour of other open-source and commercial tools; Kafdrop is one such tool, but there are several others. This book covers the packaged CLI tools because that is what you are sure to get with every Kafka installation. Having basic awareness of the built-in tooling is about as essential as knowing the basic `vi` commands when working in Linux — you can berate the archaic tooling and laud the alternatives, but that will only get you so far as your first production incident. In saying that, comparing Kafka’s built-in tooling to Vim is a travesty.

Switching to Kafdrop, we can see the `getting-started` topic appear in the “Topics” section. If there are lots of topics, you can use the filter text box in the table area to refine the displayed topics to just those that match a chosen substring.

We can tell at a glance that the topic has three partitions, the topic has no replication issues, and that no custom configuration has been specified for this topic. The mechanics for specifying per-topic configuration will be explained in Chapter 9: Broker Configuration.

Now, we could have just as easily created a topic by clicking the “New” button under the topics list. However, the point of the exercise is to demonstrate the CLI tool, rather than to explore all the possible ways one can create a topic in Kafka.

This might be a good segue to discuss the importance of explicit topic creation. Kafka does not require clients to create topics by default. When the `auto.create.topics.enable` broker configuration property is set to `true`, Kafka will automatically create the topic when clients attempt to produce, consume, or fetch metadata for a non-existent topic. This might sound like a nifty idea at first, but the drawbacks significantly outweigh the minor convenience one gets from not having to create the topic explicitly.

Firstly, Kafka’s defaults date back as far as 2011, and are generally optimised for the sorts of use cases that Kafka was originally designed for — high volume log shipping. Many things have changed since; as it happens, Kafka is no longer a one-trick pony. As such, one would typically want to configure the topic at the point of creation, or immediately thereafter — certainly before it gets a real workout.

Secondly, the partition count: Kafka allows you to specify the default number of partitions for all newly created topics using the `num.partitions` broker setting, but this is largely a meaningless number. Topics should be sized individually on the basis of expected parallelism, and a number of other factors, which are discussed in Chapter 6: Design Considerations. Specifying the partition count requires explicit topic creation. A similar statement might be made regarding the replication factor, but it is arguably easier to agree on a sensible default for the replication factor than it is for the partition count.

Finally, having Kafka auto-create topics when a client subscribes to a topic or simply fetches the topic metadata is careless, to put it mildly. A misbehaving client may initiate arbitrary metadata queries that could inadvertently create a copious number of stray topics.

### Publishing records

With the topic creation out of the way, let’s publish a few records. We are going to use the `kafka-console-producer.sh` tool:

```bash
$KAFKA_HOME/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic getting-started \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Records are separated by newlines. The key and the value parts are delimited by colons, as indicated by the `key.separator` property. For the sake of an example, type in the following — a copy-paste will do:

```text
foo:first message
foo:second message
bar:first message
foo:third message
bar:second message
```

Press `CTRL+D` when done. The terminal echoes a right angle bracket (`>`) for every record published.

> Note: the `kafka-topics.sh` tool uses the `--bootstrap-server` parameter to configure the Kafka broker list, while `kafka-console-producer.sh` uses the `--broker-list` parameter for an identical purpose. Also, `--property` arguments are largely undocumented — be prepared to Google your way around.

At this point we can switch back to Kafdrop and view the contents of the `getting-started` topic. We are presented with an overview of the topic, along with a detailed breakdown of the underlying partitions. Focusing on the partition detail, we can tell at a glance that of the three partitions, two have data and one is empty. The “first offset” and “last offset” columns correspond to the low-water and high-water marks, respectively. As the reader might recall from Chapter 3: Architecture and Core Concepts, subtracting the two yields the maximum number of records persisted in the partition. Let’s click on partition #2. Kafdrop will show the individual records, arranged in chronological order.

In case you were wondering, the arrow to the left of the record lets you expand and pretty-print JSON-encoded records. As our examples didn’t use JSON, there’s nothing to pretty-print.

### Consuming records

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started \
  --group cli-consumer \
  --from-beginning \
  --property "print.key=true" \
  --property "key.separator=:"
```

The terminal will echo the following:

```text
bar:first message
bar:second message
foo:first message
foo:second message
foo:third message
```

Because the consumer is running as a subscription, with a provided consumer group, the output will stall on the last record. The consumer will effectively tail the topic — continuously polling for new records and printing them as they arrive on the topic. To terminate the consumer, press `CTRL+D`.

Note that we specified the `--from-beginning` flag when invoking the command above. By default, a first-time consumer — for a previously non-existent group — will have its offsets reset to the topic’s high-water mark. In order to read the previously published records, we override the default offset reset strategy to tail from the topic’s low-water mark. If we run the same command again, we will see no records — the consumer will halt, waiting for the arrival of new records.

There is no `--from-end` flag. To tail from the end of the topic, simply delete the consumer offsets and start the CLI consumer. Deleting offsets and other offset manipulation commands are described in the section that follows.

Having consumed the backlog of records with the new `cli-consumer` consumer group, we can now switch back to Kafdrop to observe the addition of the new group. The new group appears in the topic overview screen, under the section “Consumers”, in the bottom-right.

Clicking through the consumer link takes us to the consumer overview. This screen enumerates over all topics within the consumer’s subscription, as well as the per-partition offsets for each topic.

In our example, the consumer offset recorded for each partition is the same as the respective high-water mark. The consumer lag is zero for each column. This is the difference between the committed offset and the high-water mark. When the lag is zero, it means that the consumer has worked through the entire backlog of records for the partition; in other words, the consumer has caught up to the producer. Lag may vary between partitions — the busier the partition, in terms of record throughput, the more likely it will accumulate lag. The aggregate lag — also known as the combined lag — is the sum of all individual per-partition lags.

Among the useful characteristics of tools such as Kafdrop and the Kafka CLI is the ability to enumerate and monitor individual consumer groups — inspect the per-partition lags and spot leading indicators of degraded consumer performance or, in the worst-case scenario, a stalled consumer. Much like any other middleware, a solid comprehension of the available tooling — be it the built-in suite or the external tools — is essential to effective operation. This is particularly crucial for overseeing mission-critical systems in production environments, where minutes of downtime and the fruitless head-scratching of the engineering and support personnel can result in significant incurred losses.

So there you have it. We have published and consumed records from a Kafka topic using the built-in CLI tools. It isn’t much, but it’s a start.

## Useful CLI commands

To close off the section on the CLI, we will take a brief look at the other useful actions that can be performed using the built-in tools.

### Listing topics

The `kafka-topics.sh` tool can be used to list topics, as per the example below.

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list \
  --exclude-internal
```

The `--exclude-internal` flag, as the name suggests, eliminates the internal topics — for example, `__consumer_offsets` — from the query results.

### Describing a topic

By passing the `--describe` flag and a topic name to `kafka-topics.sh`, we can get more detailed information about a specific topic, including the partition leaders, follower replicas, and the in-sync replica set:

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic getting-started
```

Produces:

```text
Topic: getting-started PartitionCount: 3 ReplicationFactor: 1
Configs: segment.bytes=1073741824

Topic: getting-started Partition: 0 Leader: 0 Replicas: 0 Isr: 0
Topic: getting-started Partition: 1 Leader: 0 Replicas: 0 Isr: 0
Topic: getting-started Partition: 2 Leader: 0 Replicas: 0 Isr: 0
```

### Deleting a topic

To delete an existing topic, use the `kafka-topics.sh` tool. The example below deletes the `getting-started` topic from our test cluster.

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started \
  --delete
```

Topic deletion is an asynchronous operation — a topic is initially marked for deletion, to be subsequently cleaned up by a background process at an indeterminate time in the future. In-between the marking and the final deletion, a topic might appear to linger around — only to disappear moments later.

The asynchronous behaviour of topic deletion should be taken into account when dealing with short-lived topics — for example, when conducting an integration test. The latter typically requires a state reset between successive runs, wiping associated database tables and event streams. Because there is no equivalent of a blocking `DELETE TABLE` DDL operation in Kafka, one must think outside the box. The options are:

1. Forcibly reset consumer offsets to the high-water mark prior to each test, delete the offsets, or delete the consumer group — all three will achieve equivalent results.
2. Truncate the underlying partitions by shifting the low-water mark — truncation will be described shortly.
3. Use unique, disposable topic names for each test, deleting any ephemeral topics when the test ends.

The latter is the recommended option, as it creates due isolation between tests and allows multiple tests to operate concurrently with no mutually-observable side-effects.

### Truncating partitions

Although a partition is backed by an immutable log, Kafka offers a mechanism to truncate all records in the log up to a user-specified low-water mark. This can be achieved by passing a JSON document to the `kafka-delete-records.sh` tool, specifying the topics and partitions for truncation, with the new low-water mark in the `offset` attribute. Several topic-partition-offset triples can be specified as a batch. In the example below, we are truncating the first record from `getting-started:2`, leaving records at offset `1` and newer intact.

```bash
cat << EOF > /tmp/offsets.json
{
  "partitions": [
    {"topic": "getting-started", "partition": 2, "offset": 1}
  ],
  "version": 1
}
EOF

$KAFKA_HOME/bin/kafka-delete-records.sh \
  --bootstrap-server localhost:9092 \
  --offset-json-file /tmp/offsets.json
```

In an analogous manner, we can truncate the entire partition by specifying the current high-water mark in the `offset` attribute.

### Listing consumer groups

The `kafka-consumer-groups.sh` tool can be used to query Kafka for a list of consumer groups.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

The result is a newline-separated list of consumer group names. This output format conveniently allows us to iterate over groups, enacting repetitive group-related administrative operations from a shell script.

```bash
#!/bin/bash

list_groups_cmd="$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list"

for group in $(bash -c "$list_groups_cmd"); do
  # do something with the $group variable
done
```

### Describing a consumer group

The same tool can be used to display detailed state information about each consumer group — namely, its partition offsets for the set of subscribed topics. A sample invocation and the resulting output is shown below.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group cli-consumer \
  --describe \
  --all-topics
```

Produces the following when no consumers are connected:

```text
Consumer group 'cli-consumer' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
cli-consumer    getting-started 1          0               0               0    -            -     -
cli-consumer    getting-started 0          2               2               0    -            -     -
cli-consumer    getting-started 2          3               3               0    -            -     -
```

If, on the other hand, we attach a consumer — from an earlier example, using the `kafka-console-consumer.sh` tool — the output resembles the following:

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                                                           HOST           CLIENT-ID
cli-consumer    getting-started 0          2               2               0    consumer-cli-consumer-1-077c1bf1-df64-4d3e-a479-350e962119cc  /127.0.0.1     consumer-cli-consumer-1
cli-consumer    getting-started 1          0               0               0    consumer-cli-consumer-1-077c1bf1-df64-4d3e-a479-350e962119cc  /127.0.0.1     consumer-cli-consumer-1
cli-consumer    getting-started 2          3               3               0    consumer-cli-consumer-1-077c1bf1-df64-4d3e-a479-350e962119cc  /127.0.0.1     consumer-cli-consumer-1
```

In addition to describing a specific consumer group, this tool can be used to describe all groups:

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups \
  --all-topics
```

The `--describe` flag has a complementary flag — `--state` — that drills into the present state of the consumer group. This includes the ID of the coordinator node, the assignment strategy, the number of active members, and the state of the group. These attributes are explained in greater detail in Chapter 15: Group Membership and Partition Assignment. The example below illustrates this command and its sample output.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups \
  --state
```

```text
GROUP           COORDINATOR (ID)     ASSIGNMENT-STRATEGY  STATE   #MEMBERS
cli-consumer    localhost:9092 (0)   range                Stable  1
```

### Resetting offsets

In the course of working with Kafka, we occasionally come across a situation where the committed offsets of a consumer group require minor adjustment; in the more extreme case, that adjustment might entail a complete reset of the offsets. An adjustment might be necessary if, for example, the consumer has to skip over some records — perhaps due to the records containing erroneous data. These are sometimes referred to as “poisoned” records. Alternatively, the consumer may be required to reprocess earlier records — possibly due to a bug in the application which was subsequently resolved. Whichever the reason, the `kafka-consumer-groups.sh` tool can be used with the `--reset-offsets` flag to affect fine-grained control over the consumer group’s committed offsets.

The example below rewinds the offsets for the consumer group `cli-consumer` to the low-water mark, using the `--to-earliest` flag — resulting in the forced reprocessing of all records when the consumer group reconnects. Alternatively, the `--to-latest` flag can be used to fast-forward the offsets to the high-water mark extremity, skipping all backlogged records. Resetting offsets is an offline operation; the operation will not proceed in the presence of a connected consumer.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started \
  --group cli-consumer \
  --reset-offsets \
  --to-earliest \
  --execute
```

By default, passing the `--reset-offsets` flag will result in a dry run, whereby the tool will list the partitions that will be subject to a reset, the existing offsets, as well as the candidate offsets that will be assigned upon completion. This is equivalent of running the tool with the `--dry-run` flag, and is designed to protect the user from accidentally corrupting the consumer group’s state. To enact the change, run the command with the `--execute` flag, as shown in the example above.

In addition to resetting offsets for the entire topic, the reset operation can be performed selectively on a subset of the topic’s partitions. This can be accomplished by passing in a list of partition numbers following the topic name, in the form:

```text
<topic-name>:<first-partition>,<second-partition>,...,<N-th-partition>
```

An example of this syntax is featured below. Also, rather than resetting the offset to a partition extremity, this example uses the `--to-offset` parameter to specify a numeric offset.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started:0,1 \
  --group cli-consumer \
  --reset-offsets \
  --to-offset 2 \
  --execute
```

The next example uses Kafka’s record time-stamping to locate an offset based on the given date-time value, quoted in ISO 8601 form. Specifically, the offsets will be reset to the earliest point in time that occurs at the specified timestamp or after it. This feature is convenient when one needs to wind the offsets back to a known point in time. When using the `--to-datetime` parameter, ensure that the offset is passed using the correct timezone; if unspecified, the timezone defaults to Coordinated Universal Time (UTC), also known as Zulu time. In the example below, the timezone had to be adjusted to Australian Eastern Daylight Time (AEDT), eleven hours east of Zulu, as this book was written in Sydney.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started:2 \
  --group cli-consumer \
  --reset-offsets \
  --to-datetime 2020-01-27T14:35:54.528+11:00 \
  --execute
```

The final option offered by this tool is to shift the offsets by a fixed quantity `n`, using the `--shift-by` parameter. The magnitude of the shift may be a positive number — for a forward movement, or a negative number — to rewind the offsets. The extent of the shift is bounded by the partition extremities; the result of “current offset + n” will be capped by the low-water and high-water marks.

### Deleting offsets

Another method of resetting the offsets is to delete the offsets altogether, shown in the example below. This is, in effect, a lazy form of reset — the assignment of new offsets does not occur until a consumer connects to the cluster. When this happens, the `auto.offset.reset` client property will stipulate which extremity the offset should be reset to — either the earliest offset or the latest.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic getting-started \
  --group cli-consumer \
  --delete-offsets
```

### Deleting a consumer group

Deleting a consumer group erases all persistent state associated with it. This is accomplished by passing the `--delete` flag to the `kafka-consumer-groups.sh` CLI, as shown below.

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group cli-consumer \
  --delete
```

Deleting the consumer group is equivalent to deleting offsets for all topics and all partitions.

## A basic Java producer and consumer

A CLI is a great place to start, and can be driven programmatically from a shell script. But this is more of a convenience; an automation of repetitive functionality, if you will. Any serious event streaming application will employ a high-level language such as Java, C, or Python to implement the business logic required to publish records and to react to events emitted by other applications.

### Client libraries

Unlike the built-in CLI, which relies on the presence of binaries and a pre-installed Java runtime, applications rely solely on distributable client libraries. These are available for just about every programming language under the sun, from the mainstream to the esoteric.

In this book, we are going to focus solely on the Java ecosystem — being among the most popular mainstream software development environments and the “home turf” of Kafka and many related event streaming technologies. The Java client implementation is the most mature of the available client libraries, being developed alongside and at the same cadence as the Kafka broker. Other languages will have similar clients; they are maintained independently of Kafka and feature varying levels of feature support and stability. Bear in mind, these libraries will slightly lag the mainstream Kafka releases in terms of feature sets; if you are after “bleeding edge” capabilities, you will be best served by the Java client library and, to a marginally lesser extent, the C library — `librdkafka`, maintained by Magnus Edenhill.

### Using the Java library

To add a Kafka client library to your project, add the following to your `build.gradle` if using Gradle:

```gradle
dependencies {
  implementation "org.apache.kafka:kafka-clients:2.4.0"
}
```

Alternatively, if using Maven, add the following to your `pom.xml`:

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>2.4.0</version>
</dependency>
```

The examples above assume Kafka version `2.4.0` — the latest at the time of writing. Replace this with a more up-to-date version if appropriate.

The complete source code for the upcoming examples is available at [github.com/ekoutanov/effectivekafka](https://github.com/ekoutanov/effectivekafka) in the `src/main/java/effectivekafka/basic` directory. Code listings will have their package declaration removed for brevity, and often will strip out import statements and outer class declarations.

Interfacing with the Kafka client libraries is done primarily using the following classes:

- **`Producer`**: The public interface of the producer client, containing the necessary method signatures for publishing records and using transactions. This interface is surprisingly light on documentation; method comments simply delegate the documentation to the concrete implementation.
- **`KafkaProducer`**: The implementation of `Producer`. In addition, a `KafkaProducer` contains detailed Javadoc comments for each method.
- **`ProducerRecord`**: A data structure encompassing the attributes of a record, as perceived by a producer. To be precise, this is the representation of a record before it has been published to a partition; as such, it contains only the basic set of attributes: topic name, partition number, optional headers, key, value, and a timestamp.
- **`Consumer`**: The definition of a consumer entity, containing message signatures for controlling subscriptions and topic/partition assignment, fetching records from the cluster, committing offsets, and obtaining information about the available topics and partitions.
- **`KafkaConsumer`**: The implementation of `Consumer`. Like its producer counterpart, this implementation contains the complete set of Javadocs.
- **`ConsumerRecord`**: A consumer-centric structure for housing record attributes. A `ConsumerRecord` is effectively a superset of the `ProducerRecord`, containing additional metadata such as the record offset, the checksum, and some other internal attributes.

There are other classes that are used, from time to time, to interface with the client library. However, the bulk of record publishing and consumption can be achieved using little more than just the six classes above.

### Publishing records

A simple, yet complete example illustrating the publishing of Kafka records is presented below.

```java
import static java.lang.System.*;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class BasicProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final var topic = "getting-started";

    final Map<String, Object> config = Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    try (var producer = new KafkaProducer<String, String>(config)) {
      while (true) {
        final var key = "myKey";
        final var value = new Date().toString();
        out.format("Publishing record with value %s%n", value);

        final Callback callback = (metadata, exception) -> {
          out.format("Published with metadata: %s, error: %s%n", metadata, exception);
        };

        // publish the record, handling the metadata in the callback
        producer.send(new ProducerRecord<>(topic, key, value), callback);

        // wait a second before publishing another
        Thread.sleep(1000);
      }
    }
  }
}
```

The first item on our to-do list is to configure the client. This is done by building a mapping of property names to configured values. More detailed information on configuration is presented in Chapter 10: Client Configuration; for the time being, we will limit ourselves to the most basic configuration options — just enough to get us going with a functioning producer.

The configuration keys are strings — being among the permissible property names defined in the official Kafka documentation, available online at [kafka.apache.org/documentation](https://kafka.apache.org/documentation). Rather than quoting strings directly, our example employs the static constants defined in the `ProducerConfig` class, thereby avoiding a mistype.

Of the four configuration mappings supplied, the first specifies a list of so-called bootstrap servers. In our example, this is a singleton list comprising the endpoint `localhost:9092` — the address of our test broker. Bootstrapping is a moderately involved topic, described in Chapter 8: Bootstrapping and Advertised Listeners.

The next two mappings specify the serializers that the producer should use for the records’ keys and values. Kafka offers lots of options around how keys and values are marshalled — using either built-in or custom serializers. For the sake of expediency, we will go with the simplest option at our disposal — writing records as plain strings. More elaborate forms of marshalling will be explored in Chapter 7: Serialization.

Whereas the first three items represent mandatory configuration, the fourth is entirely optional. By default, in the absence of idempotence, a producer may inadvertently publish a record in duplicate or out-of-order — if one of the queued records experiences a timeout during publishing and is reattempted after one or more of its successors have gone through.

With the `enable.idempotence` option set to `true`, the broker will maintain an internal sequence number for each producer and partition pair, ensuring that records are not processed in duplicate or out-of-order. So it’s good practice to enable idempotence.

Prior to publishing a record, we need to instantiate a `KafkaProducer`, giving it the assembled config map in the constructor. A producer cannot be reconfigured following instantiation. Once instantiated, we will keep a reference to the `KafkaProducer` instance, as it must be closed when the application no longer needs it. This is important because a `KafkaProducer` maintains TCP connections to multiple brokers and also operates a background I/O thread to ferry the records across. Failure to close the producer instance may result in resource starvation on the client, as well as on the brokers. As `Producer` extends the `Closeable` interface, the best way to ensure that the producer instance is properly disposed of is to use a try-with-resources block, as shown in the listing above.

Sometimes we need a producer to hang around indefinitely — for example, when an application publishes events in response to some external stimuli, such as responding to an API request. In this scenario, the use of a try-with-resources is inappropriate, as the lifecycle of a `KafkaProducer` instance is obviously aligned with that of the API controller or the associated business logic layer, depending on how the application is architected. Instead, we would let the owner of the producer, whichever component that may be, deal with lifecycle concerns.

In order to actually publish a record, one must use the `Producer.send()` API. There are two overloaded variations of the `send()` method:

1. `Future<RecordMetadata> send(ProducerRecord<K, V> record)`: asynchronously sends the record, returning a `Future` containing the record metadata.
2. `Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback)`: asynchronously sends the record, invoking the given `Callback` implementation when either the record has been successfully persisted on the broker or an error has occurred. Our example uses this variant.

The `send()` methods are asynchronous, returning as soon as the record is serialized and staged in the accumulator buffer. The actual sending of the record will be performed in the background, by a dedicated I/O thread. To block on the result, the application can invoke the `get()` method of the provided `Future`.

The publishing of records takes place in a loop, with a one second sleep between each successive `send()` call. For simplicity, we are publishing the current date, keyed to a constant `"myKey"`. This means that all records will appear on the same partition.

Running the example above results in the following output, until terminated:

```text
13:14:09/0 INFO [main]: [Producer clientId=basic-producer-sample]
Instantiated an idempotent producer.

13:14:09/66 INFO [main]: [Producer clientId=basic-producer-sample]
Overriding the default retries config to the recommended value of
2147483647 since the idempotent producer is enabled.

13:14:09/66 INFO [main]: [Producer clientId=basic-producer-sample]
Overriding the default acks to all since idempotence is enabled.

13:14:09/82 INFO [main]: Kafka version: 2.4.0
13:14:09/82 INFO [main]: Kafka commitId: 77a89fcf8d7fa018
13:14:09/82 INFO [main]: Kafka startTimeMs: 1570264049533

Publishing record with value Wed Jan 02 13:14:09 AEDT 2020

13:14:09/495 INFO [kafka-producer-network-thread | basic-producer-sample]:
[Producer clientId=basic-producer-sample] Cluster ID: efkResGcSUWMV6zqj9D8vw

13:14:09/497 INFO [kafka-producer-network-thread | basic-producer-sample]:
[Producer clientId=basic-producer-sample] ProducerId set to 12000 with epoch 0

Published with metadata: getting-started-0@0, error: null
Publishing record with value Wed Jan 02 13:14:10 AEDT 2020
Published with metadata: getting-started-0@1, error: null
Publishing record with value Wed Jan 02 13:14:11 AEDT 2020
Published with metadata: getting-started-0@2, error: null
Publishing record with value Wed Jan 02 13:14:12 AEDT 2020
Published with metadata: getting-started-0@3, error: null
```

### Consuming records

The following listing demonstrates how records are consumed.

```java
import static java.lang.System.*;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public final class BasicConsumerSample {
  public static void main(String[] args) {
    final var topic = "getting-started";

    final Map<String, Object> config = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer-sample",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    try (var consumer = new KafkaConsumer<String, String>(config)) {
      consumer.subscribe(Set.of(topic));

      while (true) {
        final var records = consumer.poll(Duration.ofMillis(100));
        for (var record : records) {
          out.format("Got record with value %s%n", record.value());
        }
        consumer.commitAsync();
      }
    }
  }
}
```

This example is strikingly similar to the producer — same building of a config map, instantiation of a client, and the use of a try-with-resources block to ensure the client is closed after it leaves scope.

The first difference is in the configuration. Consumers have some elements common with producers — such as the `bootstrap.servers` list, and several others — but by and large they are different. The deserializer configuration is symmetric to the producer’s serializer properties; there are key and value equivalents.

The group ID configuration is optional — it specifies the ID of the consumer group. This example uses a consumer group named `basic-consumer-sample`. The auto offset reset configuration stipulates what happens when the consumer subscribes to the topic for the first time. In this case, we would like the consumer’s offset to be reset to the low-water mark for every affected partition, meaning that the consumer will get any backlogged records that existed prior to the creation of the group in Kafka. The default setting is `latest`, meaning the consumer will not read any prior records.

Finally, the auto-commit setting is disabled, meaning that the application will commit offsets at its discretion. The default setting is to enable auto-commit with a minimum interval of five seconds.

Prior to polling for records, the application must subscribe to one or more topics using the `Consumer.subscribe()` method.

Once subscribed, the application will repeatedly invoke `Consumer.poll()` in a loop, blocking up to a maximum specified duration or until a batch of records is received. For each received record, this example simply prints the record’s value. Once all records have been printed, the offsets are committed asynchronously using the `Consumer.commitAsync()` method. The latter returns as soon as the offsets are enqueued internally; the actual sending of the commit message to the group coordinator will take place on the background I/O thread. The group coordinator is responsible for arbitrating the state of the consumer group. The reader might also recall from Chapter 3: Architecture and Core Concepts, that the repeated polling and handling of records is called the poll-process loop.

Running the example above results in the following output, until terminated:

```text
10:47:16/0 INFO [main]: Kafka version: 2.4.0
10:47:16/0 INFO [main]: Kafka commitId: 77a89fcf8d7fa018
10:47:16/0 INFO [main]: Kafka startTimeMs: 1580341636585

10:47:16/2 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Subscribed to topic(s): getting-started

10:47:17/431 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Cluster ID: efkResGcSUWMV6zqj9D8vw

10:47:18/1740 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Discovered group coordinator
172.20.40.148:9092 (id: 2147483647 rack: null)

10:47:18/1744 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] (Re-)joining group

10:47:18/1795 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] (Re-)joining group

10:47:18/1828 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Finished assignment for group at generation 1

10:47:18/1881 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Successfully joined group with generation 1

10:47:18/1884 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Adding newly assigned partitions:
getting-started-1, getting-started-0, getting-started-2

10:47:18/1900 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Found no committed offset for partition
getting-started-1

10:47:18/1900 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Found no committed offset for partition
getting-started-0

10:47:18/1900 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Found no committed offset for partition
getting-started-2

10:47:18/1921 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Resetting offset for partition
getting-started-1 to offset 0.

10:47:18/1921 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Resetting offset for partition
getting-started-0 to offset 0.

10:47:18/1921 INFO [main]: [Consumer clientId=consumer-basic-consumer-sample-1,
groupId=basic-consumer-sample] Resetting offset for partition
getting-started-2 to offset 0.

Got record with value Wed Jan 02 13:14:09 AEDT 2020
Got record with value Wed Jan 02 13:14:10 AEDT 2020
Got record with value Wed Jan 02 13:14:11 AEDT 2020
Got record with value Wed Jan 02 13:14:12 AEDT 2020
Got record with value Wed Jan 02 13:14:13 AEDT 2020
Got record with value Wed Jan 02 13:14:14 AEDT 2020
Got record with value Wed Jan 02 13:14:15 AEDT 2020
Got record with value Wed Jan 02 13:14:16 AEDT 2020
Got record with value Wed Jan 02 13:14:17 AEDT 2020
```

This chapter has hopefully served as a practical reflection on the theoretical concepts that were outlined in Chapter 3: Architecture and Core Concepts. Specifically, we learned how to interact with a Kafka cluster using two distinct, yet commonly used approaches.

The first part explored the use of the built-in CLI tools. These are basic utilities that allow a user to publish and consume records, administer topics and consumer groups, make various configuration changes, and query various aspects of the cluster state. As we have come to realise, the built-in tooling is far from perfect, but it is sufficient to carry out basic administrative operations, and at times it may be the only toolset at our disposal.

The second part looked at the programmatic interaction with Kafka, using the Java client library. We looked at simple examples for publishing and consuming records and learned the basics of the Java client API. Real applications will undoubtedly be more complex than the provided examples, but they will invariably utilise the exact same building blocks.

---

# Chapter 6: Design Considerations

Previous chapters have taken us through the essentials of event streaming and the core concepts of Kafka. By now, the reader should be familiar with the architecture of Kafka, its internal components, as well as the producer and consumer ecosystems. We have set up a Kafka broker, a Kafdrop UI and built basic producer and consumer applications using the Java client APIs.

In essence, the reader should now be equipped with the tools and foundational knowledge required to start building event streaming applications. But it takes time and experience to become proficient in Kafka. This is another way of saying: To write good software, you need to make lots of mistakes. Mistakes need to be made; they are an essential part of learning. But learning from other peoples’ mistakes is better than learning from one’s own. So this chapter presents a list of considerations that are instructive in the design of performant and sustainable event streaming applications; considerations that have been amassed over years of working with these sorts of systems across a variety of industries.

## Roles and responsibilities

Kafka permits a flexible arrangement between producers and consumers, allowing for a host of similar and disparate applications to interact with a topic simultaneously. In coming to terms with this, an often-asked question is: Which party owns the topic, and who is responsible for its upkeep?

### Event-oriented broadcast

In a broadcast arrangement, where the producer-consumer relationship follows a (multi)point-to-multipoint topology, it is an accepted best-practice for the producer ecosystem to assume custodianship over the topic, and to effectively prescribe the entirety of the topic’s configuration and usage semantics. These include:

- The lifecycle of the topic, as well as the associated broker-side configuration, such as the retention period and compaction policy;
- The nature and content of the published data, encodings, record schema, versioning strategy and associated deprecation period; and
- The sizing of the topic with respect to the partition count and the keying of the records.

In no uncertain terms: the producer is king. The producer will warrant all existential and behavioural aspects of the topic; the only decision left to the discretion of the consumer is whether to subscribe to the topic or not. This, rather categorical, approach to role demarcation is essential to preserving the key characteristic of an event-driven architecture — loose coupling. The producer cannot be intrinsically aware of the topic’s consumers, as doing so would largely defeat the intent of the design.

This is not to say that producers should publish on a whim, or that the suitability of the published data is somehow immaterial to the outcome. Naturally, the published data should be correct, complete, and timely; however, the assurance of this lies with the designers of the system and is heavily predicated on the efficacy of domain modelling and stakeholder consultation. It is also evolutionary in nature; feedback from the consuming parties during the design, development, and operation phases should be used to iteratively improve the data quality. Stated otherwise, while the consuming parties are consulted as appropriate, the final decision rights and associated responsibilities rest with the producer.

### Peer-to-peer messaging

Kafka may be used in a peer-to-peer messaging arrangement, whereby the consumer is effectively responding to specific commands issued by a producer, and in most cases emitting responses back to the initiator. This model sees a role reversal: the consumer plays the role of the service provider, and therefore assumes custody over the lifecycle of the topic and its defining characteristics.

Where the response is sent over a different topic, shared among message initiators, the semantics of the response topic are also fully defined by the consumer. In more elaborate messaging scenarios, the initiator of the request may ask that the response is ferried over a dedicated topic to avoid sharing; in this case, the lifecycle of the topic and its retention will typically be managed by the initiator, while record-related aspects remain within the consumer’s remit.

### Topic conditioning

Reading the section on producer-driven topic modelling may fail to instill confidence in would-be consumers. The flip side of the coupling argument is the dreaded leap of faith. If the prerogative of the producer is to optimise topics around its domain model, what measures exist to assure the consumers that the upstream decisions do not impact them adversely? Is a compromise possible, and how does one neutralise the apparent bias without negatively impacting all parties?

These are fair questions. In answering, the reader is invited to consider the case where there are multiple disparate consumers. Truly, a single-producer-multiple-consumers is a fairly routine arrangement in contemporary event-driven architecture. And this is precisely the use case that highlights why a compromise is not a viable option. As the number of disagreeing parties grows, the likelihood of striking an effective compromise decreases to the point where the resulting solution is barely tractable for either party. It is the architectural equivalent of children fighting over a stuffed toy, where the inevitable outcome is the tearing of the toy, the dramatic scattering of its plush contents, resulting in discontent but ultimately quiesced children.

So what is one to do?

While this problem might not be trivially solvable, it can be readily compartmentalised. The use of a staged event-driven architecture (SEDA) offers a way of managing the complexity of diverse consumer requirements without negatively impacting the consumer applications directly or coupling the parties. Rather than feeding consumers directly off the producer-driven topic, intermediate processing stages are employed to condition the data to conform to an individual consumer group’s expectations. These stages are replicated for each independent set of consumers.

Under this model, the impedance mismatch is resolved by the intermediate stages, leading to improved maintainability of the overall solution and allowing each of the end-parties to operate strictly within the confines of their respective domain models. The responsibilities of the parties are unchanged; the consumer takes ownership of the conditioning stage, responsible for its development and upkeep. While this might initially appear like a zero-sum transfer, the benefit of this approach is in its modularity. It embraces the single responsibility principle, does not clutter the consumer with transformational logic, and can lead to a more sustainable solution.

The acquired modularity may also lead to opportunities for component reuse. If two or more distinct consumer groups share similar data requirements, a common conditioning stage can power both.

## Parallelism

It was previously stated that exploiting partial order enables consumer parallelism. The distribution of partition assignments among members of a consumer group is the very mechanism by which this is achieved. While consumer load-balancing is straightforward in theory, the practical implications of aspects such as topic sizing, record key selection, and consumer scaling are not so apparent.

There are several factors one must account for when designing highly performant event streaming applications. The following is an enumeration of some of these factors.

### Producer-driven partitioning

Irrespective of the particular messaging topology employed, the responsibility of assigning records to partitions lies solely with the producer. This stems from a fundamental design limitation of Kafka; both topics and partitions are physical constructs, implemented as segmented log files under the hood. The publishing of a record results in the appending of a serialized form of the record to the head-end of an appropriate log file. Once this occurs, the relationship between a record and its encompassing topic-partition is cemented for the lifetime of the record.

In Kafka terminology, the most recent records are considered to be at the “head” end of a partition.

The implication of this constraint is that the producer should take the utmost care in keying the records such as to preserve the essential causal relations, without overly constraining the record order. In practice, events will relate to some stable entity; the identifier of that entity can serve as the key of the corresponding record.

By way of example, consider a hypothetical content syndication system catering to football fans. Our system integrates with various real-time content providers, listening to significant in-play events from football matches as they unfold, then publishes a consolidated event stream to power multiple downstream consumers — mobile apps, scoreboards, player and match stats, video stream overlays, social networks such as Twitter, and other subscribers. True to the principles of event-driven architecture, we try to remain agnostic of what’s downstream, focusing instead on the completeness and correctness of the emitted event stream — the data content of the records, their timing, and granularity.

Mimicking the real-life order of events is a good starting point for designing a streaming application. With this in mind, it makes sense to appoint the football match as the stable entity — its identifier will act as a record key to induce the partial order. This means that goals, corners, penalties, and so forth, will appear in the order they occurred within a match; records across matches will not be ordered by virtue of the matches being unrelated.

The basic requirement for the stability of the chosen entities is that they should persist for the lifetime of all causally related events. This does not imply that the entities should be long-lived, only that they exist long enough to survive the causal chains that depend on them.

Assuming no special predecessor-successor relationship between the chosen stable entities, the partial order of the resulting records will generally be sufficient for most, if not all, downstream consumers. Conversely, if the entities themselves exhibit causal relationships, then the resulting stream may fail to capture the complete causality.

Where a predecessor-successor relationship exists between stable entities, and that relationship is significant to downstream subscribers, there are generally two approaches one may take. The first is to coarsen the granularity of event ordering, for example, using the tournament identifier as the record key. This preserves the chronological order of in-play events within a tournament, and by extension, within a match. The second approach is to transfer the responsibility of event reordering to the downstream subscriber.

Coarsening of causal chains is generally preferred when said order is an intrinsic characteristic of the publisher’s domain. The main drawback of this approach is the reduced opportunity for consumer parallelism, as coarsening leads to a reduction in the cardinality of the partitioning key. Subscribers will not be able to utilise Kafka’s load-balancing capabilities to process match-level substreams in parallel; only tournaments will be subject to parallelism.

An extension of this model is to introduce conditioning stages for those subscribers that would benefit from the finer granularity. A conditioning stage would consume a record from a coarse-grained input topic, then republish the same value to an output topic with a different key — for example, switching the key from a tournament ID to a match ID.

The second approach — fine-grained causal chains with consumer-side reordering — assumes that the consumer, or some intermediate stage acting on its behalf, is responsible for coarsening the granularity of the event substreams to fit some bespoke processing need. The conditioning stage will need to be stateful, maintaining a staging datastore for incoming events so that they can be reordered. In practice, this is much more difficult than it appears. In some cases, there simply isn’t enough data available for a downstream stage to reconstruct the original event order. In short, consumer-side reordering may not always be a viable option.

There is no one-size-fits-all approach to partitioning domain events. As a rule of thumb, the producer should publish at the finest level of granularity that makes sense in its respective domain, while supporting a diverse subscriber base. A crucial point: being agnostic of subscribers does not equate to being ignorant of their needs. The effective modelling of the domain and the associated event streams fall on the shoulders of architects and senior technologists; stakeholder consultation and understanding of the overall landscape are essential in constructing a sustainable solution.

In the absence of consumer awareness, one litmus test for formulating partial order is to ascertain that the resulting stream can be used to reconstitute the source domain. Ask the question: Can a hypothetical subscriber rebuild an identical replica of the domain purely from the emitted events while maintaining causal consistency? If the answer is “yes”, then the event stream should be suitable for any downstream subscriber. Otherwise, if the answer is “no”, then there is a gap in the condition of the emitted events that requires attention.

### Topic width

With the correct keying granularity in place, the next consideration is the “width” of the topic — the number of partitions it encompasses. Assuming a fair distribution of keys, increasing the number of partitions creates more opportunities for the consumer ecosystem to process records in parallel. Getting the topic width right is essential to a performant event streaming architecture.

Regrettably, Kafka does not make this easy for us. Kafka only permits non-destructive resizing of topics when increasing the partition count. Decreasing the number of partitions is a destructive operation — requiring the topic to be created anew, and manually repopulated.

One of the main gotchas of resizing topics is the effect they have on record order. As stated in Chapter 3: Architecture and Core Concepts, a Kafka producer hashes the record’s key to arrive at the partition number. The hashing scheme is not consistent — two records with the same key hashed at different points in time will correspond to an identical partition number if and only if the number of partitions has not changed in that time. Increasing the partition count results in the two records occupying potentially different partitions — a clear breach of Kafka’s key-centric ordering guarantee. Kafka will not rehash records as part of resizing, as this would be prohibitively expensive in the absence of consistent hashing.

> When the correctness of a system is predicated on the key-centric ordering of records, avoid resizing the topic as this will effectively void any ordering guarantees that the consumer ecosystem may have come to rely upon.

One approach to dealing with prospective growth is to start with a sufficiently over-provisioned topic, perhaps an order of magnitude more partitions than one would reasonably expect — thereby avoiding the hashing skew problem down the track. On the flip side, increasing the number of partitions may increase the load on the brokers and consumers.

A partition is backed by log files, which require additional file descriptors. At minimum, there is one log segment and one index file per partition. Inbound writes flow to dedicated buffers, which are allocated per partition on the broker. Therefore, increasing the number of partitions, in addition to consuming extra file handles, will result in increased memory utilisation on the brokers. A similar impact will be felt on consumers, sans the file handles, which also employ per-partition buffers for fetching records.

A further impact of wide topics may be felt due to the limitations of the inter-broker replication process and its underlying threading model. Specifically, a broker will allocate one thread for every other broker that it maintains a connection with, which covers the set of replicated partitions — where the two peers have a leader-follower relationship. The replication threads may act as a bottleneck when shuttling records from the partition leader to in-sync replicas, and thereby impact the publishing latency when the producer requests all replicas to acknowledge the writes.

Confluent — one of the major contributors to Apache Kafka — recommends limiting the number of partitions per broker to: `100 × b × r`, where `b` is the number of brokers in a Kafka cluster and `r` is the replication factor.

So while a wider topic provides for greater theoretical throughput, it does carry practical and immediate implications on the client and broker performance. The impacts of widening individual topics may not be substantial, but they may be felt in aggregate. This is not to say that topics should not be over-provisioned; rather, decisions regarding the sizing of the topics and the extent of over-provisioning should not be taken on a whim. The broker topology and the overall capacity of the cluster play a crucial role; these have to be adequately specified and taken into consideration when sizing the topics.

If dealing with an existing topic that has saturated its capacity for consumer parallelism, consider a staged destructive resize. Create a new topic of the desired size, using a tool such as MirrorMaker to replicate the topic contents onto the wider topic. When the replication catches up, switch the producers to double-publish to both the old and the new topics. Individual consumer groups can start migrating to the new topic at their discretion; however, the misalignment of partitions may present a challenge with persisted offsets. Assuming that consumers have been designed with idempotency in mind, one should be able to set the `auto.offset.reset` property to `earliest` to force the reprocessing of the records from the beginning.

### Scaling of the consumer group

Kafka will allocate partitions approximately evenly among members of a consumer group, up to the width of the topic. So to increase parallelism, one must ensure sufficient consumer instances in the group. Allocating a fixed number of instances to the group is usually not economical, as it may result in idle capacity, particularly for event streams that exhibit cyclic or bursty loading. The recommended approach is to employ an automated horizontal scaling technique to dynamically expand or contract the population of the group in response to load demand. For example, if deploying the consumer group within a public cloud environment like AWS, one may use an autoscaling group to provision additional instances based on CPU utilisation metrics. Alternatively, if the consumer application is containerised, the use of a container orchestration platform is recommended. For example, when deployed in Kubernetes, one would employ horizontal pod autoscaling to dynamically size the consumer group.

### Internal consumer parallelism

An alternate way of increasing consumer throughput, without widening the topic or scaling the consumer group, is to exploit parallelism within the consumer process. This can be achieved by partitioning the workload among a pool of threads by independently hashing the record keys to maintain local order. This strategy would be classed as vertical scaling, requiring increased parallelism on each consumer node in exchange for reducing the number of consumers, and hence the number of partitions.

## Idempotence and exactly-once delivery

Chapter 3: Architecture and Core Concepts had introduced the concepts of delivery guarantees, stating that Kafka allows for two different modes of delivery by simply shifting the point when the consumer commits its offsets. At-least-once and at-most-once guarantees were named, but there was no mention of an exactly-once guarantee. This might be a good segue to discuss the differences between delivery modes; ultimately, it will help us understand what it means to do something “exactly once”.

The role of messaging middleware is to decouple communications between collaborating parties. When a sender publishes a message, there is an assumption that the receiver — or receivers, as there may be multiple such parties — will eventually consume and process the message. Messaging middleware is generally divided into two categories: those that offer at-most-once and those that offer at-least-once guarantees. And then we have Kafka, which has a foot in each camp.

The at-most-once guarantee simply means that a message is never redelivered to its recipient, no matter the contingency. The consumer might read the record, but then fail for whatever reason before processing the record. If the offsets for the said record were committed before the record was processed, then the reassignment of the partition following the consumer’s failure will result in the skipping of the record by the new consumer.

The at-least-once guarantee means that a message will only be marked as delivered when it completes its entire journey within the consumer application. Failure prior to this point is treated as non-delivery and a retry will ensue.

Using the at-most-once approach for delivery is acceptable in many cases, especially where the occasional loss of a record does not leave a system in a perpetually inconsistent state. At-most-once delivery is useful where the source of the record is continuously, within a fixed interval, emitting updates to some entity of interest, such that the loss of one record can be recovered from in bounded time. Conversely, the at-least-once approach is more fitting where the loss of a record constitutes an irreversible loss of data, violating some fundamental invariant of the system. But the flip side is that processing a record multiple times may introduce undesirable side-effects. This is where the notion of exactly-once processing enters the scene. In fact, when contrasting at-least-once with at-most-once delivery semantics, an often-asked question is: Why can’t we just have it once?

Without delving into the academic details, which involve conjectures and impossibility proofs, it is sufficient to say that exactly-once semantics are not possible without tight-knit collaboration with the consumer application. As disappointing as it may sound, a messaging platform cannot offer exactly-once guarantees on its own. What does this mean in practice?

To achieve the coveted exactly-once semantics, consumers in event streaming applications must be idempotent. In other words, processing the same record repeatedly should have no net effect on the consumer ecosystem. If a record has no additive effects, the consumer is inherently idempotent. For example, if the consumer simply overwrites an existing database entry with a new one, then the update is naturally idempotent. Otherwise, the consumer must check whether a record has already been processed, and to what extent, prior to processing the record. The combination of at-least-once delivery and consumer idempotence collectively leads to exactly-once semantics.

The design of an idempotent consumer mandates that all effects of processing a record must be traceable back to the record. For example, a record might require updating a database, invoking some service API, or publishing one or more records to a set of downstream topics. The latter is particularly common in SEDA systems, which are essentially graphs of processing nodes joined by topics. When a consumer processes a record, it will have no awareness of whether the record is being processed for the first time, or whether the given record is a repeat attempt of an earlier failed delivery. As such, the consumer must always assume that a record is a duplicate, and handle it accordingly. Every potential side-effect must be checked to ensure that it hasn’t already occurred, before attempting it a second time. When a side-effect is itself idempotent, then it can be repeated unconditionally.

In some cases, there may not be an easy way to determine whether a potential side-effect had already occurred as a result of a previous action. For example, the side-effect might be to publish a record on another topic; there is often no practical way of querying for the presence of a prior record. Kafka offers an advanced mechanism for correlating the records consumed from input topics with the resulting records on output topics — this is discussed in Chapter 18: Transactions. Transactions can create joint atomicity and isolation around the consumption and production of records, such that either all scoped actions appear to have occurred, or none.

Where the target endpoint is a non-Kafka message queue, the downstream receiver must be made idempotent. In other words, two or more identical records with different offsets must not result in material duplication somewhere down the track. This is called end-to-end idempotence. As the name suggests, this guarantee spans the entirety of an event-streaming graph, covering all nodes and edges. In practice, this is achieved by ensuring that any two neighbouring nodes have an established mechanism for idempotent communication.

This chapter has explored some of the fundamental considerations pertinent to the design and construction of safe and performant event streaming applications.

We started by covering the roles and responsibilities of the various parties collaborating in the construction of distributed event-driven applications. The key takeaway is that the parties publishing or consuming events can be likened to service providers and invokers, and their roles vary depending on the messaging topology. We also explored scenarios where producers and consumers might disagree on the domain model, and the methods by which this can be resolved.

The concept of key-centric record parallelism — Kafka’s trademark performance enhancer — has been explored. We looked at the factors that constrain the consumers’ ability to process events in parallel, and the design considerations that impact the producing party.

Finally, we contrasted at-most-once and at-least-once delivery guarantees and arrived at the design requirements for exactly-once — namely, a combination of at-least-once delivery and consumer idempotence.

---

# Chapter 7: Serialization

The examples we have come across so far demonstrated fundamental Kafka producer and consumer behaviour by serializing basic types, such as strings. While this may be sufficient to garner an introductory level of awareness, it is of limited use in practice, as real-life applications rarely publish or consume unstructured strings.

Distributed applications communicating in either a message-passing style or as part of an event-driven architecture will utilise a broad catalogue of structured datatypes and corresponding schema contracts. The business logic embedded in producer and consumer applications will typically deal with native domain models, requiring a bridging mechanism to marshal these models to Kafka topics when producing records, and perform the opposite when consuming from a topic.

This chapter covers the broad topic of record serialization. In the course of the discussion, we shall also explore complementary design patterns that streamline the interfacing of an application’s business logic with the underlying event stream.

## Key and value serializer

Prior examples have revealed that the Kafka `Producer` and `Consumer` API, as well as the `ProducerRecord` and `ConsumerRecord` classes, are generically typed. The `Producer` interface is parametrised with a key and a value type, denoted `K` and `V` in the type parameter list.

Kafka’s ingrained type-safety mechanism assumes a pair of compatible serializers for supported key and value types. A custom serializer must conform to the `org.apache.kafka.common.serialization.Serializer` interface.

Serializers are configured in one of two ways:

1. Passing the fully-qualified class name of a `Serializer` implementation to the producer via the `key.serializer` and the `value.serializer` properties.
2. Directly instantiating the serializer and passing it as a reference to an overloaded `KafkaProducer` constructor.

The property-based mechanism has the advantage of simplicity, in that it lives alongside the rest of the producer configuration. One can look at the configuration properties and instantly determine that the producer is configured with a specific key and value serializer. The drawback of this configuration style is that it requires the `Serializer` implementation to include a public, no-argument constructor. It also makes it difficult to configure. Because the serializer is instantiated reflectively by the producer client, the application code is unable to inject its own set of arguments at the point of initialisation. The only way to configure a reflectively-instantiated serializer is to supply a set of custom properties to the producer, then retrieve the values of these properties in the `Serializer` implementation, using the optional `configure()` callback.

The Kafka client library comes with several pre-canned serializers for common data types. In most applications, record keys are simple unstructured values such as integers, strings, or UUIDs, and a built-in serializer will suffice. Record values tend to be structured payloads conforming to some pre-agreed schema, represented using a text or binary encoding. Typical examples include JSON, XML, Avro, Thrift, and Protocol Buffers.

When serializing a custom payload to a Kafka record, there are generally two approaches one may pursue:

1. Implement a custom serializer to directly handle the payload.
2. Serialize the payload at the application level.

The first approach is idiomatic; unquestionably, it is more fitting to the design of the Kafka API. We would subclass `Serializer`, implementing its `serialize()` method. The `serialize()` method accepts the `data` argument that is typed in accordance with the generic type constraint of the `Serializer` interface. From there it is just a matter of marshalling the payload to a byte array.

The alternate method involves piggybacking on an existing serializer that matches the underlying encoding. When dealing with text-based formats, such as JSON or XML, one would use the `StringSerializer`. Conversely, when dealing with binary data, the `ByteArraySerializer` would be selected. This leaves Kafka’s `ProducerRecord` and `Producer` instance unaware of the application-level datatype, relying on the application code to pre-serialize the value before constructing a `ProducerRecord`.

A potential advantage of a custom Kafka serializer over application-level serialization is the additional type-safety that the former offers. Looking at it from a different lens, the need for type-safety at the level of a Kafka producer is questionable, as it would likely be encapsulated within a dedicated messaging layer. This is a good segue into layering. A well-thought-out application will clearly separate business logic from the persistence and messaging concerns. By the same token, it makes sense for the `Producer` class to also be encapsulated in its own layer, ideally using an interface that allows the messaging code to be mocked out independently as part of unit testing.

Returning to the question of a custom serializer versus a piggybacked approach, the former is the idiomatic approach, and for this reason we will stick with custom serializers and deserializers throughout the chapter.

## Sending events

For the forthcoming discussion, consider a contrived event streaming scenario involving a basic application for managing customer records. Every change to the customer entity results in the publishing of a corresponding event to a single Kafka topic, keyed by the customer ID. Each event is strongly typed, but there are several event classes and each is bound to a dedicated schema. The POJO representation of these events might be `CreateCustomer`, `UpdateCustomer`, `SuspendCustomer`, and `ReinstateCustomer`. The abstract base class for all customer-related events will be `CustomerPayload`. The base class also houses the common fields, which for the sake of simplicity have been reduced to a single UUID-based unique identifier. This is the ID of the notional customer entity to which the event refers.

We are going to assume that records should be serialized using JSON. Along with Avro, JSON is one of the most popular formats for streaming event data over Kafka. The examples in this book use the FasterXML Jackson library for working with JSON, which is the de facto JSON parser within the Java ecosystem. Subclasses of `CustomerPayload` are specified using a `@JsonSubTypes` annotation, which allows us to use Jackson’s built-in support for polymorphic types. Every serialized `CustomerPayload` instance will contain a `type` property, specifying an aliased name of the concrete type. Jackson uses this property as a hint during deserialization, picking the correct subclass of `CustomerPayload` to map the JSON document to.

Ideally, we would like to inject a high-level event sender into the business logic, then have our business logic invoke the sender whenever it needs to produce an event, without concerning itself with how the event is serialized or published to Kafka. This is the perfect case for an interface:

```java
public interface EventSender extends Closeable {
  Future<RecordMetadata> send(CustomerPayload payload);

  default RecordMetadata blockingSend(CustomerPayload payload)
      throws SendException, InterruptedException {
    try {
      return send(payload).get();
    } catch (ExecutionException e) {
      throw new SendException(e.getCause());
    }
  }
}
```

The application might want to send records asynchronously — continuing without waiting for an outcome, or synchronously — blocking until the record has been published. We have specified a `Future<RecordMetadata> send(CustomerPayload payload)` method signature for the asynchronous operation. The synchronous case is taken care of by the `blockingSend()` method, which simply delegates to `send()`, blocking on the result of the returned `Future`.

Next, we are going to look at a sample user of `EventSender` — the `ProducerBusinessLogic` class. We are not actually going to implement any life-like business logic for this example; the intention is merely to simulate some activity and exercise our future `EventSender` implementation.

### The complete sender

Using interfaces is only going to get us so far; we need a concrete implementation of an `EventSender` to make the example work. Here is the simplest sender implementation that will get the job done:

```java
public final class DirectSender implements EventSender {
  private final Producer<String, CustomerPayload> producer;
  private final String topic;

  public DirectSender(Map<String, Object> producerConfig, String topic) {
    this.topic = topic;
    final var mergedConfig = new HashMap<String, Object>();
    mergedConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    mergedConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerPayloadSerializer.class.getName());
    mergedConfig.putAll(producerConfig);
    producer = new KafkaProducer<>(mergedConfig);
  }

  @Override
  public Future<RecordMetadata> send(CustomerPayload payload) {
    final var record = new ProducerRecord<>(topic, payload.getId().toString(), payload);
    return producer.send(record);
  }

  @Override
  public void close() {
    producer.close();
  }
}
```

There really isn’t much to it. The `DirectSender` encapsulates a `KafkaProducer`, configured using a supplied map of properties. The constructor will overwrite certain key properties in the user-specified configuration map — properties that are required for the correct operation of the sender and should not be interfered with by external code. The `send()` method simply creates a new `ProducerRecord` and enqueues it for sending, delegating to the underlying `Producer` instance. Before this, the `send()` method will also assign the newly created record’s key, which, as previously agreed, is expected of the producer application. By setting the key to the customer ID, we ensure that records are strictly ordered by customer.

The benefits of layering become immediately apparent. Without an `EventSender` implementation to guide the construction and sending of records, the responsibility of enforcing invariants would have rested with the business logic layer. This prevents us from enforcing simple invariants that operate at record scope, such as “the key of a record must equal to the ID of the encompassed customer event”. Relying on the business logic to set the key correctly is error-prone, especially when you consider that there will be several places where this would be done. By layering our producer application, we can enforce this behaviour deeper in the stack, thereby minimising code duplication and avoiding a whole class of potential bugs.

## Key and value deserializer

Analogously to the generic type constraints prevalent in the producer API, the `Consumer` interface enforces an equivalent constraint vis-à-vis the `ConsumerRecords` class returned by the `poll()` method, which carries a collection of individual `ConsumerRecord` objects. Similarly to the producer scenario, a consumer must be configured with the appropriate key and value deserializers. A deserializer must conform to the `org.apache.kafka.common.serialization.Deserializer` interface.

Akin to the serialization scenario, the user can select one of two strategies for unmarshalling data:

1. Implement a custom deserializer to directly handle the encoded form, such that the application code deals exclusively with typed payloads.
2. Piggyback on an existing deserializer, such as a `StringDeserializer` for text encodings or a `ByteArrayDeserializer` for binary encodings, deferring the final unmarshalling of the encoded payload to the application.

There are no strong merits of one approach over the other that are worthy of a debate. Like in the producer scenario, we will use a custom deserializer to implement the forthcoming examples, being the idiomatic approach.

## Receiving events

Continuing from the producer example, let’s examine the routine concerns of a typical business logic layer that might reside in a consumer application. How would it react to events received from Kafka? And more importantly, how would it even receive these events?

The standard mechanism for interacting with a Kafka consumer is to block on `Consumer.poll()`, then iterate over the returned records — invoking an application-level handler for each record. Kafka’s defaults around automatic offset committing have also been designed specifically around this pattern — the poll-process loop. A poll-process loop requires a thread on the consumer, as well as all the life-cycle management code that goes with it — when to start the thread, how to stop it, and so on.

Ideally, we would simply inject a high-level event receiver into the business logic, then register a listener callback with the receiver to be invoked every time the receiver pulls a record from the topic. Perhaps something along these lines:

```java
public interface EventReceiver extends Closeable {
  void addListener(EventListener listener);
  void start();
  void close();
}

@FunctionalInterface
public interface EventListener {
  void onEvent(CustomerPayload payload);
}
```

This approach completely decouples the consumer business logic from the consumer code, being aware only of `EventReceiver`, which in itself is merely an interface. All communications with Kafka will be proxied via a suitable `EventReceiver` implementation.

### Corrupt records

A producer has the benefit of knowing that the records given to it by the application code are valid, at least as far as the application is concerned. A consumer reading from an event stream does not have this luxury. A rogue or defective producer may have published garbage onto the topic, which would be summarily fed to all downstream consumers. Ideally, we should handle any potential deserialization issues gracefully. As deserialization is within our control, we have several choices around the error-handling behaviour: just log the error and discard the record, propagate the error to the application via the modified callback, or publish the malformed record to a dedicated dead-letter topic for subsequent inspection.

Assuming the decision is to pass the error to the application, the modified code might resemble the following:

```java
@FunctionalInterface
public interface EventListener {
  void onEvent(ReceiveEvent event);
}

public final class ReceiveEvent {
  private final CustomerPayload payload;
  private final Throwable error;
  private final ConsumerRecord<String, ?> record;
  private final String encodedValue;

  public ReceiveEvent(CustomerPayload payload, Throwable error, ConsumerRecord<String, ?> record, String encodedValue) {
    this.record = record;
    this.payload = payload;
    this.error = error;
    this.encodedValue = encodedValue;
  }

  public boolean isError() { return error != null; }
  // getters omitted...
}
```

The new `ReceiveEvent` class encapsulates both the `CustomerPayload` object — if one was unmarshalled successfully, or a `Throwable` error — if an exception occurred during unmarshalling. In both cases, the original `ConsumerRecord` is also included for reference, as well as the original encoded value.

### The complete receiver

Now, to complete the implementation, we require a functioning `EventReceiver`. The listing below is that of the `DirectReceiver`, which is an implementation of the poll-process loop. The `DirectReceiver` maintains a single polling thread. Rather than incorporating threading from first principles, the examples in this book use a micro-library for worker thread management.

The `onPollCycle()` method represents a single iteration of the poll-process loop. Its role is straightforward — fetch records from Kafka, construct a corresponding `ReceiveEvent`, and dispatch the event to all registered listeners. Once all records in the batch have been dispatched, the `commitAsync()` method of the consumer is invoked, which will have the effect of asynchronously committing the offsets for all records fetched in the last call to `poll()`. Being asynchronous, the client will dispatch the request in a background thread, not waiting for the commit response from the brokers.

The use of `commitAsync()` makes it possible to process multiple batches before the effects of committing the first are reflected on the brokers, increasing the window of uncommitted records. And while this will lead to a greater number of replayed records following partition reassignment, this behaviour is still consistent with the concept of at-least-once delivery. Using the blocking `commitSync()` variant reduces the number of uncommitted records to the in-flight batch at the expense of throughput. Unless the cost of processing a record is very high, the asynchronous commit model is generally preferred.

Notice how we have caught an odd-looking `org.apache.kafka.common.errors.InterruptException` in the body of the `onPollCycle()` method, re-throwing a `java.lang.InterruptedException` in its place. This is one of the idiosyncrasies of the Kafka API — its origins are traceable to Scala, which does not support checked exceptions. The standard Java thread interrupt signalling has been unceremoniously discarded in the bowels of the `KafkaConsumer` client and replaced with a bespoke runtime exception type. The code above corrects for this, trapping the bespoke exception and re-throwing a standard one.

## Pipelining

One material argument for layering a consumer application stems from the realm of performance optimisation, namely a technique called pipelining. A pipeline is a decomposition of a sequential process into a set of chained stages, where the output of one stage is fed as the input to the next via an intermediate bounded buffer. Each stage functions semi-independently of its neighbours; it can operate for as long as at least one element is available in its input buffer and will also halt for as long as the output buffer is full.

Pipelining allows the application to recruit additional threads — increasing the throughput at the expense of processor utilisation. The `KafkaConsumer` implementation utilises a rudimentary form of pipelining under the hood, prefetching and buffering records to accelerate content delivery. The main poller thread will invoke `KafkaConsumer.poll()`, wait for the outcome of a pending fetch, decompress the batch, deserialize each record, and then the application applies the requisite business logic. The last step in the poll-process loop is optional, in that we could have just deferred to the consumer’s built-in automatic offset committing feature.

While the `KafkaConsumer` allows for pipelining via its prefetch mechanism, the implementation stacks the deserialization of records and their subsequent handling onto a single thread of execution. The thread that is responsible for deserializing the records is also used to drive business logic. Both operations are potentially time-consuming; when a record is being deserialized, the polling thread is unable to execute the `EventListener` callbacks, and vice versa.

While we cannot control this element of the client’s standard behaviour, we can make greater use of the pipeline pattern, harnessing additional performance gains by separating record deserialization from payload handling. The fetching, deserialization, and processing of records has now been separated into three stages, each powered by a dedicated thread. For simplicity, we are going to refer to these as the I/O thread, the polling thread, and the processing thread.

The polling thread is altered in two crucial ways:

1. Rather than invoking the `EventListener`, the thread will append the received record onto a bounded buffer.
2. Instead of committing the offsets of the recent batch, the polling thread will commit just those offsets that have been appended to the pending offsets queue by the processing thread.

On the processing thread, we have the following steps:

1. Remove the queued record from the bounded buffer.
2. Invoke the registered `EventListener` callbacks to process the record.
3. Having processed the record, append a corresponding entry to the pending offsets queue.

When pipelining records, one needs to take particular care when committing the records’ offsets, as the records might not be processed for some time after being queued. The failure of the consumer application would lead to missed records. To achieve at-least-once delivery semantics, the offsets of a record must be committed at some point after the record is processed. While disabling `enable.auto.commit` is optional in the direct consumer scenario, it must categorically be disabled in the pipeline scenario.

The need to shuttle the offsets back to the I/O thread addresses an inherent limitation of the `KafkaConsumer` implementation. Namely, the consumer is not thread-safe. Attempting to invoke `commitAsync()` from a thread that is different to the one that invoked `poll()` will result in a `java.util.ConcurrentModificationException` exception. As such, we have no choice but to repatriate the offsets to the polling thread.

The final point — the addition of one to a record’s offset — accounts for the fact that a Kafka consumer will start processing records from the exact offset persisted against its encompassing consumer group. Naively committing the record’s offset “as is” will result in the replaying of the last committed record following a topic rebalancing event. By adding one to the offset, we are ensuring that the new assignee will skip over the last processed record.

By applying the pipeline pattern, we have decoupled two potentially slow operations, allowing them to operate independently of one another. The performance gains are not exclusive to multi-core or multi-processor architectures; even single-core, pseudo-concurrent systems will benefit from pipelining by maximising the amount of useful work a processor can do. One of the perceived drawbacks of pipelining is that it adds latency to the process; the contention over a shared buffer and the overhead of thread scheduling and cache coherence will add to the end-to-end propagation delay. And while the added latency is typically more than made up for in throughput gains, it is really up to the application designer to make the final call on the optimisation strategy.

## Record filtering

In rounding off this chapter, we shall highlight another compelling reason for an abstraction layer: the filtering of records. Filtering fulfills a set of use cases where either a deserializer, or an application-level unmarshaller might conditionally present a record to the rest of the application. This is not a native capability of a Kafka consumer, requiring a bespoke implementation.

The natural question one might ask is: Why not filter at the business logic layer with some `if` statements? There are two challenges with this approach, which also become more difficult to solve as one moves up the application stack. Firstly, it assumes that the client has the requisite domain objects that can be mapped from a record’s serialized form. Secondly, it incurs the performance overhead of unconditionally unmarshalling all records, only to discard some records shortly thereafter.

The first problem can be attributed to several causes: the source Kafka topic is broadly-versed, containing more types of records that the consumer legitimately requires; the record types might be known to the consumer, but it may have no interest in processing them; or the record structure has evolved over time, such that the topic may contain records that comply to varying schema versions.

Kafka’s idiomatic approach for dealing with varying record representations is through custom serializers and deserializers. Once implemented and configured, serializers and deserializers work behind the scenes, accepting and delivering application-native record keys and values via a generically typed API. The producer application is expected to address the `Producer` implementation directly, while on the consumer-end, this approach is often paired with a simple poll-process loop.

This chapter has explored some of the typical concerns of producer and consumer applications, arguing for the use of an abstraction layer to separate Kafka-specific messaging code from the business logic. This makes it easier to encapsulate common behaviour and invariants on one hand, and on the other, simplifies key aspects of the application, making it easier to mock and test in isolation. We have also come to understand the inefficiency inherent in the poll-process loop, namely the stacking of record deserialization and processing onto a single execution thread. Finally, we have looked at record versioning and filtering as prime use cases for concealing non-trivial behaviour behind an abstraction layer.

---

# Chapter 8: Bootstrapping and Advertised Listeners

Who are these listeners? And what are they advertising? Having been in the Kafka game since 2015, without exaggeration, the most common question that gets asked is: “Why can’t I connect to my broker?” And it is typically followed up with: “I’m sure the firewall is open; I tried pinging the box; I even tried telnetting into port 9092, but I still can’t connect.” The bootstrapping configuration will frustrate the living daylights out of most developers and operations folk at some point.

## A gentle introduction to bootstrapping

Before we can start looking into advertised listeners, we need a thorough understanding of the client bootstrapping process. As it was previously stated, Kafka replicates a topic and its underlying partitions among several broker nodes, such that one broker will act as a leader for one partition and a follower for several others. Assuming that a topic has many partitions and the allocation of replicas is approximately level, no single broker will master a topic in its entirety.

Now let’s take the client’s perspective for a moment. When a producer wishes to publish a record, it must contact the lead broker for the target partition, which in turn, will disseminate the record to the follower replicas, before acknowledging the write. Since a producer will typically publish on all partitions at some point, it will require a direct connection to most, if not all, brokers in a Kafka cluster.

Unfortunately Kafka brokers are incapable of forwarding a write request to the lead broker. When attempting to publish a record to a broker that is not a declared partition leader within a replica set, the latter will fail with a `NOT_LEADER_FOR_PARTITION` error. This error sits in the category of retryable errors, and may occur from time to time, notably when the leadership status transitions among the set of in-sync replicas.

The challenge boils down to this: How does a client discover the nodes in a Kafka cluster? A naive solution would have required us to explicitly configure the client with the complete set of individual addresses of each broker node. Establishing direct connections would be trivial, but the solution would not scale to a dynamic cluster topology; adding or removing nodes from the cluster would require a reconfiguration of all clients.

The solution that Kafka designers went with is based on a directory metaphor. Rather than being told the broker addresses, clients look up the cluster metadata in a directory in the first phase in the bootstrapping process, then establish direct connections to the discovered brokers in the second phase. Rather than coming up with more moving parts, the role of the directory is conveniently played by each of the brokers. Since brokers are intrinsically aware of one another via ZooKeeper, every broker has an identical view of the cluster metadata, encompassing every other broker, and is able to impart this metadata onto a requesting client. Still, the brokers might change, which seemingly contradicts the notion of a stable directory. To counteract this, clients are configured with a bootstrap list of broker addresses that only needs to be partially accurate. As long as one address in the bootstrap list points to a live broker, the client will learn the entire topology.

### Taking advantage of DNS

You would be right in thinking that this model feels brittle. What if we recycle all brokers in a cluster? What if the brokers are hosted on ephemeral instances in the Cloud and may come and go as they please, with a new IP address each time? The bootstrap list would soon become useless. Aren’t we only kicking the reconfiguration can down the road?

While there is no official response to this, the practice adopted in the community is to use a second tier of DNS entries. Suppose we had an arbitrarily-sized cluster that could be recycled on demand. Each broker would be assigned an IP address and likely an auto-generated hostname, both being ephemeral. To complement the directory metaphor, we would create a handful of well-known canonical DNS CNAME or A records with a minimal TTL, pointing to either the IP addresses or the hostnames of a subset of our broker nodes.

An elaboration of the above technique is to use round-robin DNS. Rather than maintaining multiple A records for unique hosts, DNS permits several A records for the same host, pointing to different IP addresses. A DNS query for a host will return all matching A records, permuting the records prior to returning. Assuming the client will try the first IP address in the returned list, each address will serve an approximately equal number of requests.

The ability to utilise all resolved addresses was introduced to Kafka in version 2.1, as part of KIP-302. To maintain backward-compatible behaviour, Kafka disables this by default. To enable this feature, set the `client.dns.lookup` configuration to `use_all_dns_ips`. Once enabled, the client will utilise all resolved DNS entries. The advantage of this approach is that it does not require us to alter the bootstrap list when adding more fallback addresses.

## A simple scenario

In a simple networking topology, where each broker can be reached on a single address and port number, the bootstrapping mechanism can be made to work with minimal configuration. Consider a simple scenario with three brokers confined to a private network, such that the producer and consumer clients are also deployed on the same network. Keeping things simple, let’s assume the broker IP addresses are `10.10.0.1`, `10.10.0.2`, and `10.10.0.3`. Each broker is listening on port `9092`. A client application deployed on `10.20.0.1` is attempting to connect to the cluster.

At this point one would naturally assume that passing in `10.10.0.1:9092,10.10.0.2:9092,10.10.0.3:9092` for the bootstrap list should just work. The problem is that the Kafka broker does not know which IP address or hostname it should advertise, and it does a pretty bad job at auto-discovering this. In most cases, it will default to `localhost`.

Upon bootstrapping, the client will connect to `10.10.0.1:9092`, being the first element in the bootstrap list. Having made the connection, the client will receive the cluster metadata — a list of three elements — each being `localhost:9092`. You can see where this is going. The client will then try connecting to `localhost` — to itself. Et voila, that is how the dreaded error is obtained: `Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.`

This is as much of a problem for simple single-broker Kafka installations as it is for multi-broker clusters. Clients will always follow addresses revealed by the cluster metadata even if there is only one node. This is solved with advertised listeners. Finally, we are getting around to the crux of the matter.

A Kafka broker may be configured with three properties — `advertised.listeners`, `listeners`, and `listener.security.protocol.map` — which are interrelated and designed to be used in concert. The `listeners` property is structured as a comma-separated list of URIs, which specify the sockets that the broker should listen on for incoming TCP connections. Each URI comprises a free-form protocol name, followed by `://`, an optional interface address, followed by a colon, and finally a port number. Omitting the interface address will bind the socket to the default network interface. Alternatively, you can specify the `0.0.0.0` meta-address to bind the socket on all interfaces.

The protocol name must map to a valid security protocol in the `listener.security.protocol.map` property. The security protocols are fixed, constrained to the following values:

- `PLAINTEXT`: Plaintext TCP connection without user principal authentication.
- `SSL`: TLS connection without authentication.
- `SASL_PLAINTEXT`: Plaintext connection with SASL to authenticate user principals.
- `SASL_SSL`: The combination of TLS for transport-level security and SASL for user principal authentication.

In addition to specifying a socket listener in `listeners`, you need to state how the listener is advertised to producer and consumer clients. This is done by appending an entry to `advertised.listeners`, in the form of: `<listener protocol>://<advertised host name>:<advertised port>`.

Returning to our earlier example, we would like the first broker to be advertised on `10.10.0.1:9092`. So we would edit `server.properties` to the tune of: `advertised.listeners=PLAINTEXT://10.10.0.1:9092`. Note: There was no need to change `listeners` or `listener.security.protocol.map` because we didn’t introduce a new listener; we simply changed how the existing listener is advertised.

How does this fix bootstrapping? The client will still connect to a random host specified in the bootstrap list. This time, the cluster metadata returned by the host will contain the correct client-reachable addresses and port numbers of broker nodes, rather than a set of `localhost:9092` entries. Now the client is able to establish direct connections, provided that these addresses are reachable from the client.

## Multiple listeners

The simple example discussed earlier applies when there is a single ingress point into the Kafka cluster; every client, irrespective of their type or deployment location, accesses the cluster via that ingress. What if you had multiple ingress points? Suppose our three-broker cluster is deployed in a virtual private cloud, VPC, on AWS. Most clients are also deployed within the same VPC. However, a handful of legacy consumer and producer applications are deployed outside the VPC in a managed data centre. There are no private links, VPN or Direct Connect, between the VPC and the data centre.

One approach is to expose the brokers to the outside world via an Internet Gateway, such that each broker has a pair of addresses — an internal address and an external address. Assume that security is a non-issue for the moment — we just want to connect to the Kafka cluster over the Internet. The internal addresses will be lifted from the last example, while the external ones will be `200.0.0.1`, `200.0.0.2`, and `200.0.0.3`.

The situation is resolved by adding a second listener, targeting the external ingress. We would have to modify our `server.properties` to resemble the following:

```properties
listeners=INTERNAL://:9092,EXTERNAL://:9093
advertised.listeners=INTERNAL://10.10.0.1:9092,EXTERNAL://200.0.0.1:9093
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

Rather than calling our second listener `PLAINTEXT2`, we’ve gone with something sensible — the listener protocols were named `INTERNAL` and `EXTERNAL`. The `advertised.listeners` property is used to segregate the metadata based on the specific listener that handled the initial bootstrapping connection from the client. In other words, if the client connected on the `INTERNAL` listener socket bound on port `9092`, then the cluster metadata would contain `10.10.0.1:9092` for the responding broker as well as the corresponding `INTERNAL` advertised listener addresses for its peer brokers. Conversely, if a client was bootstrapped to the `EXTERNAL` listener socket on port `9093`, then the `EXTERNAL` advertised addresses are served in the metadata.

Individual listener configuration for every Kafka broker node is persisted centrally in the ZooKeeper cluster and is perceived identically by all Kafka brokers. Naturally, this implies that brokers must be configured with identical listener names; otherwise, each broker will serve different cluster metadata to their clients. In addition to the changes to `listeners` and `advertised.listeners`, corresponding entries are also required in the `listener.security.protocol.map`.

Keeping with the tradition of dissecting one Kafka feature at a time, we have gone with the simplest `PLAINTEXT` connections in this example. From a security standpoint, this is clearly not the approach one should take for a production cluster. Security protocols, authentication, and authorization will be discussed in Chapter 16: Security.

Clients are not the only applications connecting to Kafka brokers. Broker nodes also form a mesh network, connecting to one another to satisfy internal replication objectives — ensuring that writes to partition leaders are reflected in the follower replicas. The internal mesh connections are referred to as inter-broker communications, and use the same wire protocol and listeners that are exposed to clients. Since we changed the internal listener protocol name from the default `PLAINTEXT` to `INTERNAL`, we had to make a corresponding change to the `inter.broker.listener.name` property.

A port may not be bound to by more than one listener on the same network interface. As such, the port number must be unique for any given interface. If leaving the interface address unspecified, or if providing a `0.0.0.0` meta-address, one must assign a unique port number for each listener protocol. In our example, we went with `9092` for the internal and `9093` for the external route.

## Listeners and the Docker Network

These days it’s common to see a complete application stack deployed across several Docker containers linked by a common network. Starting with local testing, tools like Docker Compose make it easy to wire up a self-contained application stack. Taking it up a notch, orchestration platforms like Kubernetes, OpenShift, Docker Swarm, and AWS ECS add auto-scaling, zero-downtime deployments, and service discovery into the mix.

A solid understanding of Kafka’s listener and client bootstrapping mechanism is essential to deploying a broker in a containerised environment. The upcoming example will illustrate the use of multiple listeners in a basic application stack, comprising ZooKeeper, Kafka, and Kafdrop. Docker Compose will bind everything together.

To get started, create a `docker-compose.yaml` file in a directory of your choice, containing the following snippet:

```yaml
version: "3.2"
services:
  zookeeper:
    image: bitnami/zookeeper:3
    ports:
      - 2181:2181
    environment:
      ALLOW_ANONYMOUS_LOGIN: "yes"
  kafka:
    image: bitnami/kafka:2
    ports:
      - 9092:9092
    environment:
      KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181
      ALLOW_PLAINTEXT_LISTENER: "yes"
      KAFKA_LISTENERS: INTERNAL://:29092,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
    depends_on:
      - zookeeper
  kafdrop:
    image: obsidiandynamics/kafdrop:latest
    ports:
      - 9000:9000
    environment:
      KAFKA_BROKERCONNECT: kafka:29092
    depends_on:
      - kafka
```

Then bring up the stack by running `docker-compose up`. This must be run from the same directory where the `docker-compose.yaml` resides. Once it boots, navigate to `localhost:9000` in your browser. You should see the Kafdrop landing screen. It’s the same Kafdrop application as in the previous examples; the only minor difference is the value of the “Bootstrap servers”. In this example, we are bootstrapping Kafdrop using `kafka:29092` — being the internal ingress point to the Kafka broker. The term “internal” here refers to all network traffic originating from within the Compose stack. Containers attached to the Docker network are addressed simply by their service name, while the mechanics of Docker Compose by default prevent the traffic from leaving the Docker network.

Externally, that is, outside of Docker Compose, we can access both Kafka and Kafdrop with the aid of the port bindings defined in `docker-compose.yaml` file. Let’s find out if this actually works. Create a test topic using the Kafka CLI tools:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --replication-factor 1 --partitions 4
```

Now try listing the topics:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You should see a single entry echoed to the terminal: `test`. Switch back to your browser and refresh Kafdrop. As expected, the `test` topic appears in the list. Dissecting the `docker-compose.yaml` file, we set up three services. The first is `zookeeper`, launched using the `bitnami/zookeeper` image. The next service is `kafka`, which declares its dependence on the `zookeeper` service. Finally, `kafdrop` declares its dependence on `kafka`. The `kafka` service presents the most elaborate configuration of the three. The Bitnami Kafka image allows the user to override values in `server.properties` by passing environment variables. This configuration should be familiar to the reader — it is effectively identical to our earlier example, where traffic was segregated using the `INTERNAL` and `EXTERNAL` listener protocol names.

Bootstrapping is a complex, multi-stage process that enables clients to discover and maintain connections to all brokers in a Kafka cluster. It is complex not just in its internal workings; there really is a fair amount of configuration one must come to terms with in order to comfortably operate a single Kafka instance or a multi-node cluster, whether it be exposed on one or multiple ingress points. This chapter has taken us through the internal mechanics of bootstrapping. We came to appreciate the design limitations of Kafka’s publishing protocol and how this impacts the client-broker relationship, namely, requiring every client to maintain a dedicated connection to every broker in the cluster. We established how clients engage brokers in directory-style address lookups, using cluster metadata to learn the broker topology and adapt to changes in the broker population. Various traffic segregation scenarios were discussed, exploring the effects of the `listeners` and `advertised.listeners` configuration properties on how cluster metadata is crafted and served to the clients.

---

# Chapter 9: Broker Configuration

Reminiscing on our Kafka journey, so far we have mostly gotten away with running a fairly vanilla broker setup. The exception, of course, being the tweaks to the `listeners` and `advertised.listeners` configuration properties that were explored in the course of Chapter 8: Bootstrapping and Advertised Listeners. As one might imagine, Kafka offers a myriad of configuration options that affect various aspects of its behaviour, ranging from the substantial to the minute. The purpose of this chapter is to familiarise the reader with the core configuration concepts, sufficient to make one comfortable in making changes to all aspects of Kafka’s behaviour, using a combination of static and dynamic mechanisms, covering a broad set of configurable entities, as well as targeted canary-style updates.

## Entity types

There are four entity types that a configuration entry may apply to:

1. `brokers`: One or more Kafka brokers.
2. `topics`: Existing or future topics.
3. `clients`: Producer and consumer clients.
4. `users`: Authenticated user principals.

The `brokers` entity type can be configured either statically — by amending `server.properties`, or dynamically — via the Kafka Admin API. Other entity types may only be administered dynamically.

## Dynamic update modes

Orthogonal to the entity types, there are three dynamic update modes of configuration entries that affect broker behaviour:

1. `read-only`: The configuration is effectively static, requiring a change to `server.properties` and a subsequent broker restart to come into effect.
2. `per-broker`: May be updated dynamically for each broker. The configuration is applied immediately, without requiring a broker restart.
3. `cluster-wide`: May be updated dynamically as a cluster-wide default. May also be updated as a per-broker value for canary testing.

These modes apply to the `brokers` entity type. Each subsequent mode in this list is a strict superset of its predecessors. In other words, properties that support the `cluster-wide` mode, will also support `per-broker` and `read-only` modes. However, supporting `per-broker` does not imply supporting `cluster-wide`.

For other entity types, such as per-topic or per-user configuration, the settings can only be changed dynamically via the API. The scoping rules are also different. For example, there is no concept of per-broker configuration for topics; one can target all topics or individual topics, but the configuration always applies to the entire cluster. It would make no sense to roll out a topic configuration to just one broker. Dynamic configuration, which excludes the `read-only` mode, is persisted centrally in the ZooKeeper cluster. You don’t need to interact with ZooKeeper directly to assign the configuration entries. This option is still supported for backward compatibility; however, it is deprecated and its use is strongly discouraged. Instead, the Kafka Admin API and the built-in CLI tools allow an authorized user to impart changes to the Kafka cluster directly, which in turn, will be written back to ZooKeeper.

## Configuration precedence and defaults

There is a strict precedence order when configuration entries are applied. Stated in the order of priority, the highest being at the top, the precedence chain is made up of:

1. Dynamic per-entity configuration;
2. Dynamic cluster-wide default configuration; followed by
3. Static read-only configuration from `server.properties`.

A seasoned Kafka practitioner may have picked up on two additional, easily overlooked, configuration levels which we have not mentioned — default configuration and deprecated configuration. The default configuration is applied automatically if no other configuration can be resolved. There is a caveat: A property may be a relatively recent addition, replacing an older, deprecated property. If the newer property is not set, Kafka will apply the value of a deprecated property if one is set, otherwise, and only then, will the default value be applied.

Dynamic update modes were introduced in Kafka 1.1 as part of KIP-226 to reduce the overhead of administering a large Kafka cluster and potential for interruptions and performance degradation caused by rolling broker restarts. Some key motivating use cases included updating short-lived SSL keystores on the broker, performance-tuning based on metrics, adding/removing metrics reporters, updating configuration of all topics consistently across the cluster, updating log cleaner configuration for tuning, and updating listener/security configuration.

## Applying broker configuration

### Static configuration

The static configuration for all Kafka components is housed in `$KAFKA_HOME/config`. A broker sources its read-only static configuration from `server.properties`. With one exception, all entries in `server.properties` are optional. If omitted, the fallback chain comprising deprecated properties and the default value takes effect. The only mandatory setting is `zookeeper.connect`. It specifies the ZooKeeper connection string as a comma-separated list of `host:port` pairs, in the form: `host1:port1,host2:port2,...,hostN:portN`.

Another crucial configuration entry in `server.properties` is `broker.id`, which specifies the unique identifier of the broker within the cluster. If left unspecified or set to `-1`, the broker ID will be dispensed automatically by the ZooKeeper ensemble, using an atomically-generated sequence starting from `1001`. This can be configured by setting `reserved.broker.max.id`; the sequence will begin at one higher than this number. It is considered good practice to set `broker.id` explicitly, as it makes it easy to quickly determine the ID by looking at `server.properties`. Once the ID has been assigned, it is written to a `meta.properties` file, residing in the Kafka logs directory. This directory is specified by the `log.dirs` property, defaulting to `/tmp/kafka-logs`. While the naming might appear to suggest otherwise, the Kafka logs directory is not the same as the directory used for application logging. The logs directory houses the log segments, indexes, and checkpoints — used by the broker to persist topic-partition data. The contents of this directory are essential to the operation of the broker; the loss of the logs directory amounts to the loss of data for the broker in question.

To change the ID of an existing broker, you must first stop the broker. Then remove the `broker.id` entry from `meta.properties` or delete the file altogether. You can then change `broker.id` in `server.properties`. Finally, restart the broker. Upon startup, the Kafka application log should indicate the new broker ID.

### Dynamic configuration

Dynamic configuration is applied remotely over a conventional client connection. This can be done using a CLI tool — `kafka-configs.sh`, or programmatically — using a client library. The following examples assume that the Kafka CLI tools in `$KAFKA_HOME/bin` have been added to your path.

We are going to start by viewing a fairly innocuous configuration entry — the number of I/O threads. Let’s see if the value has been set in `server.properties`:

```bash
grep num.io.threads $KAFKA_HOME/config/server.properties
# Output: num.io.threads=8
```

Right, `num.io.threads` has been set to `8`. This means that our broker maintains a pool of eight threads to manage disk I/O. We can take a peek into the broker process to see these threads:

```bash
KAFKA_PID=$(jps -l | grep kafka.Kafka | awk '{print $1}')
jstack $KAFKA_PID | grep data-plane-kafka-request-handler
```

Indeed, eight threads have been spawned and are ready to handle I/O requests. Let’s use the `--describe` switch to list the dynamic value. The property name passed to `--describe` is optional. If omitted, all configuration entries will be shown.

```bash
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --describe num.io.threads
# Output: Configs for broker 0 are:
```

It’s come up empty. That’s because there are no matching per-broker dynamic configuration entries persisted in ZooKeeper. Let’s change the `num.io.threads` value by invoking the following command:

```bash
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --add-config num.io.threads=4
# Output: Completed updating config for broker: 0.
```

Re-run the describe command. Not only is it now telling us that a per-broker entry for `num.io.threads` has been set, but it is also echoing the static value and the default. View the threads again using the `jstack` command. Bingo! The number of threads has been reduced, with the action taking effect almost immediately following the update of the dynamic configuration property.

Dynamic configuration is an impressively powerful but dangerous tool. A bad value can take an entire broker offline or cause it to become unresponsive. As a precautionary measure, Kafka caps changes of certain numeric values to either half or double their previous value, forcibly smoothening out changes in the configuration. Increasing a setting to over double its initial value or decreasing it to under half of its initial value requires multiple incremental changes. Where a setting supports cluster-wide scoping, a good practice is to apply the setting to an individual broker before propagating it to the whole cluster.

The `--entity-type` argument is compulsory and can take the value of `brokers`, `users`, `clients`, or `topics`. The `--entity-name` argument is compulsory for per-broker dynamic updates and can be replaced with `--entity-default` for cluster-wide updates. Let’s apply the `num.io.threads` setting to the entire cluster.

```bash
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config num.io.threads=4
```

Then list the configuration. The resulting list has an extra value: `DYNAMIC_DEFAULT_BROKER_CONFIG:num.io.threads=4`. Having applied the cluster-wide setting, and assuming the update has proven to be stable, we can now remove the per-broker entry. Run the following:

```bash
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --alter --delete-config num.io.threads
```

Always remove per-broker settings once you apply the cluster-wide defaults. Unless there is a compelling reason for a per-broker setting to vary from a cluster-wide default, maintaining both settings creates clutter and may lead to confusion in the longer-term.

## Applying topic configuration

Settings that apply broadly to all topics, as well as topic-wide defaults, can be edited using the static configuration in `server.properties` or via dynamic updates. In addition, some settings can be altered on a per-topic basis using just the dynamic approach. The next example will tinker with another fairly benign setting — `flush.messages` — which controls the number of messages that can be written to a log before it is forcibly flushed to disk with the `fsync` command. Log flushing is disabled by default. Actually, the value is set to a very high number: 2⁶³ – 1.

Kafka requires that the topic exists before making targeted changes. We are going to create a test topic named `test.topic.config` for this demonstration.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test.topic.config --replication-factor 1 --partitions 1
```

The next command will apply an override for `flush.messages` for the `test.topic.config` topic. Note: When referring to the `topics` entity type, you must substitute the `--bootstrap-server` argument with `--zookeeper`, specifying the `host:port` combination of any node in the ZooKeeper ensemble.

```bash
kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --entity-name test.topic.config --alter --add-config flush.messages=100
```

We can now use the `--describe` switch to read back the configuration:

```bash
kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --entity-name test.topic.config --describe flush.messages
# Output: Configs for topic 'test.topic.config' are flush.messages=100
```

It was previously stated that the entity name is an optional argument to the `--describe` switch. To further broaden the query to all topics, use the `--entity-default` switch. Similarly, using `--entity-default` with the `--alter` switch will apply the dynamic configuration defaults to all topics.

Using `kafka-configs.sh` is one way of viewing the topic configuration; however, the limitation of requiring ZooKeeper for topic-related configuration prevents its use in most production environments. There are other alternatives — for example, Kafdrop — that display topic configuration using the standard Kafka Admin API.

Having done with this example, we can revert the configuration to its original state by running the command below:

```bash
kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --entity-name test.topic.config --alter --delete-config flush.messages
```

## Users and Clients

The lion’s share of configuration use cases relates to brokers, with the remainder mostly falling on topics. Users and clients are configured within the broader context of security and quota management — topic areas that will be covered separately in Chapter 16: Security and Chapter 17: Quotas. Although users and clients are not going to be covered in this chapter, their configuration closely resembles that of topics. In other words, they support two dynamic update modes: per-entity and cluster-wide, with the per-entity taking precedence, followed by cluster-wide defaults, and finally by the hard defaults.

Kafka is a highly tunable and adaptable event streaming platform. Understanding the ins and outs of Kafka configuration is essential to operating a production cluster at scale. Kafka provides a fair degree of scope granularity of an individual configuration entry. Configuration entries may apply to a range of entity types, including brokers, topics, users, and clients. Depending on the nature of the configuration, a change may be administered either statically — by amending `server.properties`, or dynamically — via the Kafka Admin API. A combination of whether remote changes are supported and the targeted scope of a configuration entry is referred to as its dynamic update mode. The broadest of supported dynamic update modes applies a cluster-wide setting to all entities of a given type. The per-entity dynamic mode allows the operator to target an individual entity — a specific broker, user, topic, or client. The read-only dynamic update mode bears the narrowest of scopes, and is reserved for static, per-broker configuration.

In addition to navigating the theory, we have also racked up some hands-on time with the `kafka-configs.sh` built-in CLI tool. This utility can be used to view and assign dynamic configuration entries. And while it is effective, `kafka-configs.sh` has its idiosyncrasies and limitations — for example, it requires a direct ZooKeeper connection for administering certain entity types.

---

# Chapter 10: Client Configuration

Some of the previous chapters have gotten us well underway in publishing and consuming records, without dwelling on the individual client properties. This is a good time to take a brief detour and explore the world of client configuration.

In contrast to broker configuration, with its dynamic update modes, selective updates, and baffling CLI tools, configuring a client is comparatively straightforward — in that client configuration is static and mostly applies to the instance being configured. (There are a few exceptions.) However, client configuration is significantly more nuanced — requiring a greater degree of insight on the user's behalf.

This chapter subdivides the configuration into producer, consumer, and admin client settings. Some configurable items are common, and have been extracted into their own section accordingly. The reader may consult this chapter in addition to the online documentation, available at [kafka.apache.org/documentation](https://kafka.apache.org/documentation). However, the analysis presented here is much more in-depth, covering material that is not readily available from official sources.

Of the numerous client configuration properties, there are several dozen that relate to performance tuning and various esoteric connection-related behaviour. The primary focus of this chapter is to cover properties that either affect the client functionally, or qualitatively impact any of the guarantees that might be taken for granted. As a secondary objective, the chapter will outline the performance implications of the configuration where the impacts are material or at least perceptible, but the intention is not to cover performance in fine detail — the user may want to defer to more specialised texts for a finer level of analysis. Other chapters will also cover aspects of Kafka's performance in more detail.

## Configuration gotchas

Client configuration is arguably more crucial than broker configuration. Broker configuration is administered by teams or individuals who are responsible for the day-to-day operation of Kafka, likely among a few other large items of infrastructure under their custodianship. Changes at this level are known or at least assumed to be broadly impacting, and are usually made with caution, with due notice given to end-users. Furthermore, the industry is seeing a noticeable shift towards fully-managed Kafka offerings, where broker operation falls under the custodianship of expert teams. Managing a single technology for a large number of clients eventually makes one proficient at it.

By comparison, there really is no such thing as a 'fully-managed Kafka client'. Clients are bespoke applications targeting specific business use cases and written by experts in the application domain. We are talking about software engineers with T-shaped skills, who are adept at a broad range of technologies and likely specialising in a few areas — probably closer to software than infrastructure.

Personal experience working with engineering teams across several clients in different industries has left a lasting impression upon the author. The overwhelming majority of perceived issues with Kafka are caused by the misuse of the technology rather than the misconfiguration of the brokers. To that point, a typical Kafka end-user knows little more than what they consider to be the minimally essential amount of insight they need for their specific uses of Kafka. And herein lies the problem: How does one assess what is essential and what is not, if they don't invest the time in exploring the breadth of knowledge at their disposal?

Software engineers working with Kafka will invariably grasp some key concepts of event streaming. A lot of that insight is amassed from conversations with colleagues, Internet articles, perusing the official documentation, and even observing how Kafka responds to different settings. Some of it may be speculative, incomplete, assumed, and in more dire cases, outright misleading. Here is an example. Most users know that Kafka offers strong durability guarantees with respect to published records. This statement is made liberally by the project's maintainers and commercial entities that derive income from Apache Kafka. This is a marketing phrase; and while it is not entirely incorrect, it has no tangible meaning and can be contorted to imply whatever the user wants it to. Can it be taken that —

1. Kafka never loses a record?
2. Kafka may occasionally lose a record, where 'occasionally' implies a tolerable level? (In which case, who decides on what is tolerable and what is not?)
3. This guarantee is applied by default to all clients and topics?
4. Kafka offers this guarantee as an option but it is up to the client to explicitly take advantage of it?

The answer is a mixture of #2 and #4, but it is much more complex than that. Granted, the cluster will set a theoretical upper bound on the durability metric, which is influenced by the number of brokers and some notional recovery point objective attributed to each broker. In other words, the configuration, topology, and hardware specification of brokers sets the absolute best-case durability rating. But it is ultimately the producer client that controls the durability of records both at the point of topic creation — by specifying the replication factor and certain other parameters, and at the point of publishing the record — by specifying the number of acknowledgements for the partition leader to request and also waiting for the receipt of the acknowledgement from the leader before deeming the record as published. Most users will take comfort in knowing they have a solid broker setup, neglecting to take the due actions on the client-side to ensure end-to-end durability.

The importance of client configuration is further underlined by yet another factor, one that stems from the design of Kafka. Given the present-day claims around the strengths of Kafka's ordering and delivery guarantees, one would be forgiven for assuming that the configuration defaults are sensible, insofar as they ought to favour safety over other competing qualities. In reality, that is not the case. Historically, Kafka was born out of LinkedIn's need to move a very large number of messages efficiently, amounting to multiple terabytes of data on an hourly basis. The loss of a message was not deemed as catastrophic, after all, a message or post on LinkedIn is hardly more than an episode of self-flattery. Naturally, this has reflected on the philosophy of setting default values that prioritise performance over just about everything else that counts. This proverbial snake-laden pit has bested many an unsuspecting engineer.

> When working with Kafka, remember the first rule of optimisation: **Don't do it.** In fairness, this rule speaks to premature optimisation; however, as it happens, most optimisation in Kafka is premature.

The good news is: Setting the configuration properties to warrant safety has only a minor impact on performance — Kafka is still a performance powerhouse.

As we explore client configuration throughout the rest of the chapter, pay particular attention to callouts that underline safety. There are a fair few, and each will have a cardinal impact on your experience with Kafka. This isn't to say that configuration 'gotchas' are exclusive to the client-side; broker configuration has them too. Comparatively, the client configuration has a disproportionate amount.

## Applying client configuration

Client configuration is assembled as a set of key-value pairs before instantiating a `KafkaProducer`, `KafkaConsumer`, or a `KafkaAdminClient` object. The original way of assembling client properties, dating to the earliest release of Kafka, was to use a `Properties` object:

```java
var props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");
props.setProperty("key.serializer", StringSerializer.class.getName());
props.setProperty("value.serializer", StringSerializer.class.getName());
props.setProperty("max.in.flight.requests.per.connection", String.valueOf(1));

try (var producer = new KafkaProducer<>(props)) {
  // do something with producer
}
```

A minor annoyance of `Properties`-based configuration is that it forces you to use a `String` type for both keys and values. This makes sense for keys, but values should just be derived from their object representation. Over time, Kafka clients have been enhanced to accept an instance of `Map<String, Object>`. Things have moved on a bit, and the same can now be written in a slightly more succinct way:

```java
Map<String, Object> config = Map.of(
    "bootstrap.servers", "localhost:9092",
    "key.serializer", StringSerializer.class.getName(),
    "value.serializer", StringSerializer.class.getName(),
    "max.in.flight.requests.per.connection", 1);

try (var producer = new KafkaProducer<>(config)) {
  // do something with producer
}
```

Frankly, whether you use `Properties` or a `Map` has no material bearing on the outcome, and is a matter of style. `Map` offers a terser syntax, particularly when using a Java 9-style `Map.of(...)` static factory method, and has the additional benefit of creating an immutable map. It is also considered the more 'modern' approach by many practitioners. On the flip side, `Properties` forces you to acknowledge that the value is a string and perform type conversion manually. The `Properties` class also has a convenient `load(Reader)` method for loading a `.properties` file. Most of the code in existence that uses Kafka still relies on `Properties`.

When a client is instantiated, it verifies that the keys correspond to valid configuration property names that are supported in the context of that client type. Failing to meet this requirement will result in a warning message being emitted via the configured logger. Let's instantiate a producer client, intentionally misspelling one of the property names:

```text
16:49:51/0 WARN [main]: The configuration
'max.in.flight.requests.per.connectionx' was supplied but
isn't a known config.

16:49:51/4 INFO [main]: Kafka version: 2.4.0
16:49:51/5 INFO [main]: Kafka commitId: 77a89fcf8d7fa018
16:49:51/5 INFO [main]: Kafka startTimeMs: 1576648191386
```

Kafka developers have opted for a failsafe approach to handling property names. Despite failing the test, the client will continue to operate using the remaining properties. In other words, there is no exception — just a warning log. The onus is on the user to inspect the configuration for correctness and sift through the application logs.

Rather than supplying an unknown property name, let's instead change the value to an unsupported type. Our original example had `max.in.flight.requests.per.connection` set to `1`. Changing the value to `foo` produces a runtime exception:

```text
Exception in thread "main" org.apache.kafka.common.config.
ConfigException: Invalid value foo for configuration
max.in.flight.requests.per.connection: Not a number of type INT
  at org.apache.kafka.common.config.ConfigDef.parseType
    (ConfigDef.java:726)
  at org.apache.kafka.common.config.ConfigDef.parseValue
    (ConfigDef.java:474)
  ...
```

There several gotchas in configuring Kafka clients, and nailing property names is among them. Chapter 11: Robust Configuration explores best-practices for alleviating the inherent naming issue and offers a hand-rolled remedy that largely eliminates the problem.

## Common configuration

This section describes configuration properties that are common across all client types, including producers, consumers, and admin clients.

### Bootstrap servers

We explored `bootstrap.servers` in Chapter 8: Bootstrapping and Advertised Listeners in considerable detail. The reader is urged to study that chapter, as it provides the foundational knowledge necessary to operate and connect to a Kafka cluster. To summarise, the `bootstrap.servers` property is mandatory for all client types. It specifies a comma-delimited list of host-port pairs, in the form `host1:port1,host2:port2,...,hostN:portN`, representing the addresses of a subset of broker nodes that the client can try to connect to, in order to download the complete cluster metadata and subsequently maintain direct connections with all broker nodes. The addresses need not all point to live broker nodes; provided the client is able to reach at least one of the brokers, it will readily learn the entire cluster topology.

The crunch is in ensuring that the retrieved cluster metadata lists broker addresses that are reachable from the client. The addresses disclosed in the metadata may be completely different from those supplied in `bootstrap.servers`. As a consequence, the client is able to make the initial bootstrapping connection but stumbles when connecting to the remaining hosts. For a better understanding of this problem and the recommended solutions, consult Chapter 8: Bootstrapping and Advertised Listeners.

### Client DNS lookup

The `client.dns.lookup` is a close relative of `bootstrap.servers`, and is also covered in Chapter 8: Bootstrapping and Advertised Listeners. The property is optional, accepting an enumerated constant from the list below.

- **`default`**: Retains legacy behaviour with respect to DNS resolution, in other words, it will resolve a single address for each bootstrap endpoint — being the first entry returned by the DNS query. This option applies both to the bootstrap list and the advertised hosts disclosed in the cluster metadata.
- **`resolve_canonical_bootstrap_servers_only`**: Detects aliases in the bootstrap list, expanding them to a list of resolved canonical names using a reverse DNS lookup. This option was introduced in Kafka 2.1.0 as part of [KIP-235](https://cwiki.apache.org/confluence/display/KAFKA/KIP-235%3A+Add+DNS+alias+support+for+secured+connection), primarily to support secured connections using Kerberos authentication. This behaviour applies to the bootstrap list only; the advertised hosts are treated conventionally, as per the `default` option.
- **`use_all_dns_ips`**: Supports multiple A DNS records for the same fully-qualified domain name, resolving all hosts for each endpoint in the bootstrap list. The client will try each host in turn until a successful connection is established. This option was introduced in Kafka 2.1.0 as part of [KIP-302](https://cwiki.apache.org/confluence/display/KAFKA/KIP-302+-+Enable+Kafka+clients+to+use+all+DNS+resolved+IP+addresses), and applies to both the bootstrap list and the advertised hosts.

### Client ID

The optional `client.id` property allows the application to associate a free-form logical identifier with the client connection, used to distinguish between the connected clients. While in most cases it may be safely omitted, the use of the client ID provides for a greater degree of source traceability, as it is used for the logical grouping of requests in Kafka metrics.

Beyond basic traceability, client IDs are also used to enforce quota limits on the brokers. The discussion of this capability will be deferred until Chapter 17: Quotas.

### Retries and retry backoff

The `retries` and `retry.backoff.ms` properties specify the number of retries for transient errors and the interval (in milliseconds) to wait before each subsequent retry attempt, respectively. The number of retries accrues on top of the initial attempt, in other words, the upper bound on the total number of attempts is `retries + 1`.

To clarify, a transient error is any condition that is deemed as potentially recoverable. Timeouts are the most common form of transient error, but there are many others that relate to the cluster state — for example, stale metadata or a controller change.

While the `retry.backoff.ms` property applies to all three client types, the `retries` property only exists for the producer and admin clients; it is not supported by the consumer client. Instead of limiting the number of retries, the consumer limits the total time accorded to a query — for example, when the `poll()` method is invoked — obviating the need for an explicit retry counter. In spite of this minor disparity, we will discuss these two configuration aspects as a collective whole.

The default setting of `retries` is `Integer.MAX_VALUE`, and the producer and admin clients will wait 100 ms by default between each attempt. There is no default value for the poll timeout that applies to consumer clients — the timeout is specified explicitly as a parameter to the `poll()` method. Whether these defaults are sensible depends on the combination of your network, the amount of resources available to both the cluster and the client apps, and your application's tolerance for awaiting a successful or failed outcome of publishing a record.

There is a gotcha here, albeit a subtle one. It does not fundamentally matter how many retries one permits, or the total time spent retrying, there are only two possible outcomes.

The number of retries and the backoff time could be kept to a minimum, in which case the likelihood of an error reaching the application is high. Even if these numbers are empirically derived, eventually one will eventually observe a scenario where the retries are exhausted.

Alternatively, one might leave `retries` at its designated default of `Integer.MAX_VALUE`, in which case the client will just keep hammering the broker, while the application fails to make progress. We need to acknowledge that failures are possible and must be accounted for at the application level.

When Kafka was first released into the wild, every broker was self-hosted, and most were running either in a corporate data centre or on a public cloud, in close proximity to the client applications. In other words, the network was rarely the culprit. The landscape has shifted considerably; it is far more common to see managed Kafka offerings, which are delivered either over the public Internet or via VPC peering. Also, with the increased adoption of event streaming in the industry, an average broker now carries more traffic than it used to, with the increase in load easily outstripping the performance advancements attributable to newer hardware and efficiency gains in the Kafka codebase. With the adoption of public cloud providers, organisations are increasingly looking to leverage availability zones to protect themselves from site failures. As a result, Kafka clusters are now larger than ever before, both in terms of the number of broker nodes and their geographic distribution. One needs to take these factors into account when setting `retries`, `retry.backoff.ms` and the consumer poll timeout, and generally when devising the error handling strategy for the application.

An alternate way of looking at the problem is that it isn't about the stability profile of the underlying network, the capacity of the Kafka cluster, or the distribution of failures one is likely to experience on a typical day. Like any other client consuming a service, one must be aware of their own non-functional requirements and any obligations they might have to their upstream consumers. If a Kafka client application is prepared to wait no more than a set time for a record to be published, then the retry profile and the error handling must be devised with that in mind.

#### Testing considerations

Continuing the discussion above, another cause of failures that is often overlooked relates to running Kafka in performance-constrained environments as part of automated testing. Developers will routinely run single-broker Kafka clusters in a containerised environment or in a virtual machine.

Kafka's I/O performance is significantly diminished in Docker or on a virtualised file system. (The reasons as to why are not important for the moment.) The problem is exacerbated when Docker is run on macOS, which additionally incurs the cost of virtualisation. As a consequence, expect a longer initial readiness time and more anaemic state transitions at the controller. The test may assume that Kafka is available because the container has started and Kafka is accepting connections on its listener ports; however, the latter does not imply that the broker is ready to accept requests. It may take several seconds for it to be ready and the timing will not be consistent from run to run. The default tolerance (effectively indefinite retries) actually copes well with these sorts of scenarios. Tuning retry behaviour to better represent production scenarios, while appearing prudent, may hamper local test automation — leading to brittle tests that occasionally fail due to timing uncertainty.

One way of solving this problem is to allow for configurable retry behaviour, which may legitimately vary between deployment environments. The problem with this approach is it introduces variance between real and test environments, which is rarely ideal from an engineering standpoint. An alternate approach, and one that is preferred by the author, is to introduce an extended wait loop at the beginning of each test, allowing for some grace time for the broker to start up. The loop can poll the broker for some innocuous read-only query, such as listing topic names. The broker may initially take some time to respond, returning a series of errors — which are ignored — while it is still in the process of starting up. But when it does respond, it usually indicates that the cluster has stabilised and the test may commence.

### Security configuration

All three client types can be configured for secure connections to the cluster. We are not going to explore security configuration in this chapter, partly because the range of supported options is overbearing, but mostly because this topic area is covered in Chapter 16: Security.

## Producer configuration

This section describes configuration options that are specific to the producer client type.

### Acknowledgements

The `acks` property stipulates the number of acknowledgements the producer requires the leader to have received before considering a request complete, and before acknowledging the write with the producer. This is fundamental to the durability of records; a misconfigured `acks` property may result in the loss of data while the producer naively assumes that a record has been stably persisted.

Although the property relates to the number of acknowledgements, it accepts an enumerated constant being one of —

- **`0`**: Don't require an acknowledgement from the leader.
- **`1`**: Require one acknowledgement from the leader, being the persistence of the record to its local log. This is the default setting when `enable.idempotence` is set to `false`.
- **`-1` or `all`**: Require the leader to receive acknowledgements from all in-sync replicas. This is the default setting when `enable.idempotence` is set to `true`.

Each of these modes, as well as the interplay between acknowledgements and Kafka's replication protocol, are discussed in detail in Chapter 13: Replication and Acknowledgements.

### Maximum in-flight requests per connection

The `max.in.flight.requests.per.connection` property sets an upper bound on the number of unacknowledged requests the producer will send on a single connection before being forced to wait for their acknowledgements. The default value of this property is `5`.

The purpose of this configuration is to increase the throughput of a producer. This is particularly evident over long-haul, high-latency networks, where long acknowledgement times continually interrupt a producer's ability to publish additional records, even if the network capacity otherwise permits this. The problem is not exclusive to high-latency networks; any internal constraint that contributes to increases in acknowledgement times — for example, slow replication within the cluster due to lagging in-sync replicas — will negatively impact the transmission rate.

This is a classic problem of flow control. Anyone familiar with the inner workings of networking protocols will immediately liken the behaviour of `max.in.flight.requests.per.connection` to the venerable sliding window protocol used for TCP's flow control. However, it is not quite the same; there is one key distinction — the lack of ordering and reassembly of in-flight records over the extent of the unacknowledged window when idempotence is disabled.

This problem is best explained with an example. Suppose a producer, configured with default values for `max.in.flight.requests.per.connection` and `retries`, queues records A, B, and C to the broker in that precise order, assuming for simplicity that the records will occupy the same partition. The tacit expectation is that these records will be persisted in the order they were sent, as per Kafka's ordering guarantees. Let's assume that A gets to the broker and is acknowledged. A transient error occurs attempting to persist B. C is processed and acknowledged. The producer, having detected a lack of acknowledgement, will retransmit B. Assuming the retransmission is successful, the records will appear in the sequence A, C, and B — distinct from the order they were sent in.

Although the previous example used individual records to illustrate the problem, it was a simplification of Kafka's true behaviour. In reality, Kafka does not forward individual records, but batches of records. But the principle remains essentially the same — just substitute 'record' for 'batch'. So rather than individual records arriving out of order, entire batches of records may appear to be reordered on their target partition.

The underlying issue is that the broker implicitly relies on ordering provided by the underlying transport protocol (TCP), which acts at Layer 4 of the OSI model. Being unaware of the application semantics (Layer 7), TCP cannot assist in the reassembly of application-level payloads. By default, when `enable.idempotence` is set to `false`, Kafka does not track gaps in transmitted records and is unable to reorder records or account for retransmissions in the face of errors.

> In scenarios where strict order is fundamental to the correctness of the system, and in the absence of idempotence, it is essential that either `retries` is set to `0` or `max.in.flight.requests.per.connection` is set to `1`. However, the preferred alternative is to set `enable.idempotence` to `true`, which will guard against the reordering problem and also avoid record duplication. This is another example where Kafka's configuration favours performance over correctness.

### Enable idempotence

The `enable.idempotence` property, when set to `true`, ensures that —

- Any record queued at the producer will be persisted at most once to the corresponding partition;
- Records are persisted in the order specified by the producer; and
- Records are persisted to all in-sync replicas before being acknowledged.

The default value of `enable.idempotence` is `false`.

Enabling idempotence requires `max.in.flight.requests.per.connection` to be less than or equal to `5`, `retries` to be greater than `0` and `acks` set to `all`. If these values are not explicitly set by the user, suitable values will be chosen by default. If incompatible values are set, a `ConfigException` will be thrown during producer initialisation.

The problem of non-idempotent producers arises when an intermittent error causes a timeout of a record acknowledgement on the return path when `acks` is set to `1` or to `all`, and `retries` is set to a number greater than zero. In other words, the broker would have received and persisted the record, but the waiting producer times out due to a delay. The producer will resend the record if it has retries remaining, which will result in a second identical copy of the record persisted on the partition at a later offset. As a consequence, all consumers will observe a duplicate record when reading from the topic. Furthermore, due to the batching nature of the producer, it is likely that duplicates will be observed as contiguous record sequences rather than one-off records.

The idempotence mechanism in Kafka works by assigning a monotonically increasing sequence number to each record, which in combination with a unique producer ID (PID), creates a partial ordering relationship that can be easily reconciled at the receiving broker. The broker maintains an internal map of the highest sequence number recorded for each PID, for each partition. A broker can safely discard a record if its sequence number does not exceed the last persisted sequence number by one. If the increment is greater than one, the broker will respond with an `OUT_OF_ORDER_SEQUENCE_NUMBER` error, forcing the batches to be re-queued on the producer. The requirement that changes to the sequence numbers are contiguous proverbially kills two birds with one stone. In addition to ensuring idempotence, this mechanism also guarantees the ordering of records and avoids the reordering issue when `max.in.flight.requests.per.connection` is set to allow multiple outstanding in-flight records.

The deduplication guarantees apply only to the individual records queued within the producer. If the application calls `send()` with a duplicate record, the producer will assume that the records are distinct, and will send the second with a new sequence number. As such, it is the responsibility of the application to avoid queuing unnecessary duplicates.

> The official documentation describes the `enable.idempotence` property as a mechanism for the producer to ensure that exactly one copy of each record is written in the stream and that records are written in the strict order they were published in.
>
> Without a suitable a priori assurance as to the liveness of the producer, the broker, and the reliability of the underlying network, the conjecture in the documentation is inaccurate. The producer is unable to enact any form of assurance if, for example, its process fails. Restarting the process would lose any queued records, as the producer does not buffer these to a stable storage medium prior to returning from `send()`. (The producer's accumulator is volatile.) Also, if the network or the partition leader becomes unavailable, and the outage persists for an extent of time beyond the maximum allowed by the `delivery.timeout.ms` property, the record will time out, yielding a failed result. In this case, the write semantics will be at most once.
>
> A degraded network or a slow broker may also present a problem. Suppose a record was published successfully, but the response timed out in such a way as to exhaust the `delivery.timeout.ms` timeout on the producer. The client will return an error to the application, which may either skip the record, or publish it a second time. In the latter case, the producer client will not detect a duplicate, and will publish what is effectively an identical record a second time. In this case, the write semantics will be at least once.
>
> Thus, the official documentation should be taken in the context of encountering intermittent errors within an otherwise functioning system, where the system is capable of making progress within all of the specified timeouts. If and only if the producer received an acknowledgement of the write from the broker, can we be certain that exactly-once write semantics were in force.

In a typical order-preserving application, setting `retries` to `0` is impractical, as it will flood the application with transient errors that could otherwise have been retried. Therefore, capping `max.in.flight.requests.per.connection` to `1` or setting `enable.idempotence` to `true` is the more sensible thing to do, with the latter being the preferred approach, being less impacted by high-latency networks.

### Compression type

The `compression.type` controls the algorithm that the producer will use to compress record batches before forwarding them on to the partition leaders. The valid values are:

- **`none`**: Compression is disabled. This is the default setting.
- **`gzip`**: Use the GNU Gzip algorithm — released in 1992 as a free substitute for the proprietary `compress` program used by early UNIX systems.
- **`snappy`**: Use Google's Snappy compression format — optimised for throughput at the expense of compression ratios.
- **`lz4`**: Use the LZ4 algorithm — also optimised for throughput, most notably for the decompression speed.
- **`zstd`**: Use Facebook's ZStandard — a newer algorithm introduced in Kafka 2.1.0, intended to achieve an effective balance between throughput and compression ratios.

This topic is discussed in greater detail in Chapter 12: Batching and Compression. To summarise, compression may offer significant gains in network efficiency. It also reduces the amount of disk I/O and storage space taken up on the brokers.

### Key and value serializer

The `key.serializer` and the `value.serializer` properties allow the user to configure the mechanism for serializing the records' keys and values, respectively. These properties have no defaults. An alternative way to specify a serializer is to directly instantiate one and pass it as a reference to an overloaded `KafkaProducer` constructor.

Serialization is a complex field that transcends client configuration, touching on the broader issues of custom data types and application design. This chapter will not discuss the nuances of serialization; instead, consult Chapter 7: Serialization for a comprehensive discussion on this topic.

### Partitioner

The `partitioner.class` property allows the application to override the default partitioning scheme by specifying an implementation of a `org.apache.kafka.clients.producer.Partitioner`. Unless instructed otherwise, the producer will use the `org.apache.kafka.clients.producer.internals.DefaultPartitioner` implementation.

The behaviour of the `DefaultPartitioner` varies depending on the attributes of the record:

1. If a partition is explicitly specified in the `ProducerRecord`, that partition will always be used.
2. If no partition is set, but a key has been specified, the key is hashed to determine the partition number.
3. If neither the partition nor the key is specified, and the current batch already has a 'sticky' partition assigned to it, then maintain the same partition number as the current batch.
4. If neither of the above conditions are met, then assign a new 'sticky' partition to the current batch and use it for the current record.

Points #1 and #2 capture the age-old behaviour of the `DefaultPartitioner`. Points #3 and #4 were added in the 2.4.0 release of Kafka, as part of [KIP-480](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner). Previously, the producer would vacuously allocate records to partitions in a round-robin fashion. While this spreads the load evenly among the partitions, it largely negates the benefits of batching. Since partitions are mastered by different brokers in the cluster, this approach used to engage potentially several brokers to publish a batch, resulting in a much higher typical latency, influenced by the slowest broker. The 2.4.0 update limits the engagement to a single broker for any given unkeyed hash, reducing the 99th percentile latency by a factor of two to three, depending on the record throughput. The partitions are still evenly loaded over a long series of batches.

Hashing a key to resolve the partition number is performed by passing the byte contents of the key through a MurmurHash2 function, then taking the low-order 31 bits from the resulting 32-bit value by masking off the highest order bit (bitwise AND with `0x7fffffff`). The resulting value is taken, modulo the number of partitions, to arrive at the partition index.

> While hashing of record keys and mapping of records to partitions might appear straightforward, it is laden with gotchas. A more thorough analysis of the problem and potential solutions are presented in Chapter 6: Design Considerations. Without going into the details here, the reader is urged to abide by one rule: **when the correctness of a system is predicated on the key-centric ordering of records, avoid resizing the topic** as this will effectively void any ordering guarantees that the consumer ecosystem may have come to rely upon.

In addition to the `DefaultPartitioner`, the Java producer client also contains a `RoundRobinPartitioner` and a `UniformStickyPartitioner`.

The `RoundRobinPartitioner` will forward the record to a user-specified partition if one is set; otherwise, it will indiscriminately distribute the writes to all partitions in a round-robin fashion, regardless of the value of the record's key. Because the allocation of unkeyed records to partitions is nondeterministic, it is entirely possible for records with the same key to occupy different partitions and be processed out of order. This partitioner is useful when an event stream is not subject to ordering constraints, in other words, when Kafka is used as a proverbial 'firehose' of unrelated events.

Alternatively, this partitioner may be used when the consumer ecosystem has its own mechanism for reassembling events, which is independent of Kafka's native partitioning scheme.

The `UniformStickyPartitioner` is a pure implementation of KIP-480 that was introduced in Kafka 2.4.0. This partitioner will forward the record to a user-specified partition if one is set; otherwise, it will disregard the key, and instead assign 'sticky' partitions numbers based on the current batch.

One other 'gotcha' with partitioners lies in them being a pure producer-side concern. The broker has no awareness of the partitioner used, it defers to the producer to make this decision for each submitted record. This assumes that the producer ecosystem has agreed on a single partitioning scheme and is applying it uniformly. Naturally, if reconfiguring the producers to use an alternate partitioner, one must ensure that this change is rolled out atomically — there cannot be two or more producers concurrently operating with different partitioners.

One can implement their own partitioner, should the need for one arise. Perhaps you are faced with a bespoke requirement to partition records based on the contents of their payload, rather than the key. While a custom partitioner may satisfy this requirement, a more straightforward approach would be to concatenate the order-influencing attributes of the payload into a synthetic key, so that the default partitioner can be used. (It may be necessary to pre-hash the key to cap its size.)

### Interceptors

The `interceptor.classes` property enables the application to intercept and potentially mutate records en route to the Kafka cluster, just prior to serialization and partition assignment. This list is empty by default. The application can specify one or more interceptors as a comma-separated list of `org.apache.kafka.clients.producer.ProducerInterceptor` implementation classes. The `ProducerInterceptor` interface is shown below, with the Javadoc comments removed for brevity.

```java
public interface ProducerInterceptor<K, V> extends Configurable {
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);

  public void onAcknowledgement(RecordMetadata metadata, Exception exception);

  public void close();
}
```

The `Configurable` super-interface enables classes instantiated by reflection to take configuration parameters:

```java
public interface Configurable {
  void configure(Map<String, ?> configs);
}
```

Interceptors act as a plugin mechanism, enabling the application to inject itself into the publishing (and acknowledgement) process without directly modifying the application code.

This naturally leads to a question: Why would anyone augment the publisher using obscurely-configured interceptors, rather than modifying the application code to address these additional behaviours directly?

Interceptors add an Aspect-Oriented Programming (AOP) style to modelling producer behaviour, allowing one to uniformly address cross-cutting concerns at independent producers in a manner that is modular and reusable. Some examples that demonstrate the viability of AOP-style interceptors include:

- Accumulation of producer metrics — tracking the total number of records published, records by category, etc.
- End-to-end tracing of information flow through the system — using correlation headers present in records to establish a graph illustrating the traversal of messages through the relaying applications, identifying each intermediate junction and the timings at each point.
- Logging of entire record payloads or a subset of the fields in a record.
- Ensuring that outgoing records comply with some schema contract.
- Data leak prevention — looking for potentially sensitive information in records, such as credit card numbers or JWT bearer tokens.

Once defined and tested in isolation, this behaviour could then be encompassed in a shared library and applied to any number of producers.

There are several caveats to implementing an interceptor:

- Runtime exceptions thrown from the interceptor will be caught and logged, but will not be allowed to propagate to the application code. As such, it is important to monitor the client logs when writing interceptors. A trapped exception thrown from one interceptor has no bearing on the next interceptor in the list: the latter will be invoked after the exception is logged.
- An interceptor may be invoked from multiple threads, potentially concurrently. The implementation must therefore be thread-safe.
- When multiple interceptors are registered, their `onSend()` method will be invoked in the order they were specified in the `interceptor.classes` property. This means that interceptors can act as a transformation pipeline, where changes to the published record from one interceptor can be fed as an input to the next. This style of chaining is generally discouraged, as it leads to content coupling between successive interceptor implementations — generally regarded as the worst form of coupling and leading to brittle code. An exception in one interceptor will not abort the chain — the next interceptor will still be invoked without having the previous transformation step applied, breaking any assumptions it may have as to the effects of the previous interceptor.
- The `onAcknowledgement()` method will be invoked by the I/O thread of the producer. This blocks the I/O thread until the method returns, preventing it from processing other acknowledgements. As a rule of thumb, the implementation of `onAcknowledgement()` should be reasonably fast, returning without unnecessary delays or blocking. It should ideally avoid any time-consuming I/O of its own. Any time-consuming operations on acknowledged records should be delegated to background threads.

In light of the above, `ProducerInterceptor` implementations should be simple, fast, standalone units of code that maintain minimal state, with no dependencies on one another. Any non-trivial interceptor implementation should have a mandatory exception handler surrounding the bodies of `onSend()` and `onAcknowledgement()`, so as to control precisely what happens in the event of an error.

### Maximum block time

The `max.block.ms` configuration property controls how long `KafkaProducer.send()` and `KafkaProducer.partitionsFor()` will block for. These methods can be blocked for two reasons: either the internal accumulator buffer is full or the metadata required for their operation is unavailable. The default value is `60000` (one minute). Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.

### Batch size and linger time

The `batch.size` and `linger.ms` properties collectively control the extent to which the producer will attempt to batch queued records in order to maximise the outgoing transmission efficiency. The default values of `batch.size` and `linger.ms` are `16384` (16 KiB) and `0` (milliseconds), respectively.

The `linger.ms` setting induces batching in the absence of heavy producer traffic by adding a small amount of artificial delay — rather than immediately sending a record the moment it is enqueued, the producer will wait for up to a set delay to allow other records to accumulate in a batch. This maximises the amount of data that can be transmitted in one go. Although records may be allowed to linger for up to the duration specified by `linger.ms`, the `batch.size` property will have an overriding effect, dispatching the batch once it reaches the set maximum size. Another way of looking at it: while the `linger.ms` property only comes into the picture when the producer is lightly loaded, the `batch.size` property is continuously in effect, ensuring the batch never grows above a set cap.

This topic is discussed in greater detail in Chapter 12: Batching and Compression. To summarise, batching improves network efficiency and throughput, at the expense of increasing publishing latency. It is often used collectively with compression, as the latter is more effective in the presence of batching.

### Request timeout

The `request.timeout.ms` property controls the maximum amount of time the client will wait for a broker to respond to an in-flight request. If the response is not received before the timeout elapses, the client will either resend the request if it has a retry budget available (configured by the `retries` property), or otherwise fail the request. The default value of `request.timeout.ms` is `30000` (30 seconds).

### Delivery timeout

The `delivery.timeout.ms` property sets an upper bound on the time to report success or failure after a call to `send()` returns, having a default value of `120000` (two minutes).

This setting acts as an overarching limit, encompassing —

- The time that a record may be delayed prior to sending;
- The time to await acknowledgement from the broker (if `acks=1` or `acks=all`); and
- The time budgeted for retryable send failures.

The producer may report failure to send a record earlier than this time if either an unrecoverable error is encountered, the retries have been exhausted, or the record is added to a batch which reached an earlier delivery expiration deadline. The value of this property should be greater than or equal to the sum of `request.timeout.ms` and `linger.ms`.

This property is a relatively recent addition, introduced in Kafka 2.1.0 as part of [KIP-91](https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer). The main motivation was to consolidate the behaviour of several related configuration properties that could potentially affect the time a record may be in a pending state, and thereby inadvertently extend this time beyond the application's tolerance to obtain a successful or failed outcome of publishing the record. The consolidated `delivery.timeout.ms` property acts as an overarching budget on the total pending time, terminating the publishing process and yielding an outcome at or just before this time is expended. In doing so, it does not deprecate the underlying properties. In fact, it prolongs their utility by making them safer to use in isolation, allowing for a more confident fine-tuning of producer behaviour.

The individual stages that constitute the record's journey from the producer to the broker are explained below.

- The initial call to `send()` can block up to `max.block.ms`, waiting on metadata or queuing for available space in the producer's accumulator. Upon completion, the record is appended to a batch.
- The batch becomes eligible for transmission over the wire when either `linger.ms` or `batch.size` has been reached.
- Once the batch is ready, it must wait for a transmission opportunity. A batch may be sent when all of the following conditions are met:
  - The metadata for the partition is known and the partition leader has been identified;
  - A connection to the leader exists; and
  - The current number of in-flight requests is less than the number specified by `max.in.flight.requests.per.connection`.
- Once the batch is transmitted, the `request.timeout.ms` property limits the time that the producer will wait for an acknowledgement from the partition leader.
- If the request fails and the producer has one or more retries remaining, it will attempt to send the batch again. Each send attempt will reset the request timeout, in other words, each retry gets its own `request.timeout.ms`.

The await-send stage is the most troublesome section of the record's journey, as there is no way to precisely determine how long a record will spend in this state. Firstly, the batch could be held back by an issue in the cluster, beyond the producer's control. Secondly, it may be held back by the preceding batch, which is particularly likely when the `max.in.flight.requests.per.connection` property is set to `1`.

Prior to Kafka 2.1.0, time spent in the await-send stage used to be bounded by the same transmission timeout that is used after the batch is sent — `request.timeout.ms`. The producer would eagerly start the transmission clock when the record entered await-send, even though technically it was still queued on the producer. Records blocked waiting for a metadata refresh or for a prior batch to complete could be pessimistically expired, even though it was possible to make progress. This problem was compounded if the prior batch was stuck in the sending stage, which granted it additional time credit — amplified by the value of the `retries` property. In other words, it was possible for an await-send batch to time out _before_ its immediate forerunner, if the preceding batch happened to have made more progress.

The strength of the `delivery.timeout.ms` property is that it does not discriminate between the various stages of a record's journey, nor does it unfairly penalise records due to contingencies in a preceding batch.

### Transactional ID and transaction timeout

The `transactional.id` and `transaction.timeout.ms` properties alter the behaviour of the producer with respect to transactions. Transactions would be classed as a relatively advanced topic on the Kafka 'complexity' spectrum; please consult Chapter 18: Transactions for a more in-depth discussion.

## Consumer configuration

This section describes configuration options that are specific to the consumer client type. Some configuration properties are reciprocals of their producer counterparts; these will be covered first.

### Key and value deserializer

Analogously to the producer configuration, the `key.deserializer`, and the `value.deserializer` properties specify the mechanism for deserializing the records' keys and values, respectively. As per the producer scenario, a user can alternatively instantiate the deserializers directly and pass them as references to an overloaded `KafkaConsumer` constructor.

A broader discussion of (de)serialization is presented in Chapter 7: Serialization. The material presented in that chapter should be consulted prior to implementing custom (de)serializers.

### Interceptors

The `interceptor.classes` property is analogous to its producer counterpart, specifying a comma-separated list of `org.apache.kafka.clients.consumer.ConsumerInterceptor` implementations, allowing for the inspection and possibly mutation of records before a call to `Consumer.poll()` returns them to the application.

The `ConsumerInterceptor` interface is shown below, with the Javadoc comments removed for brevity.

```java
public interface ConsumerInterceptor<K, V> extends Configurable, AutoCloseable {
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records);

  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets);

  public void close();
}
```

The use of interceptors on the consumer follows the same rationale as we have seen on the producer. Specifically, interceptors act as a plugin mechanism, offering a way to uniformly address cross-cutting concerns at independent consumers in a manner that is modular and reusable.

There are similarities and differences between the producer and consumer-level interceptors. Beginning with the similarities:

- Runtime exceptions thrown from the interceptor will be caught and logged, but will not be allowed to propagate to the application code. A trapped exception thrown from one interceptor has no bearing on the next interceptor in the list: the latter will be invoked after the exception is logged.
- When multiple interceptors are registered, their `onConsume()` method will be invoked in the order they were specified in the `interceptor.classes` property, allowing interceptors to act as a transformation pipeline. The same caveat applies as per the producer scenario; this style of chaining is discouraged, as it leads to content coupling between successive interceptor implementations — resulting in brittle code.
- The `onCommit()` method may be invoked from a background thread; therefore, the implementation should avoid unnecessary blocking.

As for the differences, there is one: unlike `KafkaProducer`, the `KafkaConsumer` implementation is not thread-safe — the shared use of `poll()` from multiple threads is forbidden. Therefore, there is no requirement that a `ConsumerInterceptor` implementation must be thread-safe.

### Controlling the fetch size

When retrieving records from a Kafka cluster, one ultimately needs to decide how much data is enough, and how much is too much. More is not always better; while increasing the fetch size will lead to improved network utilisation and therefore higher throughput, it comes at a price — the end-to-end propagation delay will suffer as a result. Conversely, fetching just a few records will make the application more responsive, but small fetches require more round-trips to move the same amount of data, negatively impacting the throughput.

Tuning the fetch size is among the performance-impacting decisions one has to face when building consumer applications. Kafka does little to help simplify this process. There is no consolidated 'throughput ↔ latency' dial that one can adjust to their satisfaction; instead, there are numerous consumer settings that collectively impact the fetch behaviour.

The most apparent control is the timeout parameter to the `Consumer.poll()` method. However, this is only an upper bound on the time that the method call will block for; its effects are limited to the consumer — it does not limit the amount of data retrieved from the brokers. There are several other consumer properties that influence the fetching of data; their influence extends past the client behaviour, affecting how the brokers respond to fetch queries.

The first pair of properties is the `fetch.min.bytes` and `fetch.max.bytes`. Respectively, these properties constrain the minimum and the maximum amount of data the broker should return for a fetch request.

If insufficient data is available, the request will wait for the quantity of data specified by `fetch.min.bytes` to accumulate before answering the request. The default setting is `1`, meaning that a broker will respond as soon as a single byte is available, or if the fetch request times out. The latter is governed by a separate but complementary `fetch.max.wait.ms` property, which defaults to `500` (milliseconds).

The `fetch.max.bytes` property sets a soft upper bound on the fetch request, acting more as a guide than a limit. Records are written and fetched in batches, which are treated as indivisible units from a broker's perspective. Considering that the size of a single record might conceivably eclipse the `fetch.max.bytes` limit, and indeed, the size of a given batch might also be correspondingly larger, the fetch mechanism allows for this — potentially returning a larger batch than what was specified by `fetch.max.bytes`. In doing so, it allows the consumer to make progress, which would otherwise be indefinitely obstructed had the `fetch.max.bytes` limit been enforced verbatim. More accurately, the query permits an 'oversized' batch if it is the first batch in the first non-empty partition. The default value of `fetch.max.bytes` is `52428800` (50 MiB).

The reason that batches are not broken up into individual records and returned in sub-batch quantities is due to Kafka's fundamental architecture and deliberate design decisions that contribute to its performance characteristics. Batches are an end-to-end construct; formed by the producer, they are transported, persisted, and delivered to the consumers as-is — without unpacking the batch or inspecting its contents — minimising unnecessary work on the broker. In the best-case scenario, a batch is persisted and retrieved using zero-copy, where transfer operations between the network and the storage devices occur with no involvement from the CPU. Brokers are often the point of contention — they form a static topology that does not scale elastically — unlike, say, consumers. Reducing their workload by imparting more work onto the producer and consumer clients leads to a more scalable system.

A further refinement of `fetch.max.bytes` is the `max.partition.fetch.bytes` property, applying a soft limit on a per-partition basis. The default value of `max.partition.fetch.bytes` is `1048576` (1 MiB).

There are no official guidelines for tuning Kafka with respect to `fetch.max.bytes` and `max.partition.fetch.bytes`. While their individual functions are clear, their mutual relationship is not as apparent. One must consider what happens when a fetch response aggregates batches from multiple partitions. Suppose a topic is unevenly loaded, where relatively few partitions collectively carry more records than the remaining majority of partitions. If the `max.partition.fetch.bytes` setting is overly relaxed, the results of the fetch will be biased towards the heavily-loaded partitions. In other words, the fetch quota set by `fetch.max.bytes` will be disproportionately exhausted by the minority partitions. In the best case, this will negatively affect the propagation latencies of the majority partitions; in the worst case, this might lead to periods of starvation, where records for certain partitions are unceasingly dropped from the response.

This Pareto Effect is actually more common than one might imagine. As record keys tend to reflect the identifiers of real-world entities, the distribution of records within a Kafka topic often acquires an uncanny resemblance to the real world, which as we know, is often accurately described by the power law. Setting a conservative value for `max.partition.fetch.bytes` improves fairness, increasing the likelihood of aggregating data over the majority partitions by penalising heavily loaded partitions. However, an overly conservative value undermines the allowance set by `fetch.max.bytes`. Furthermore, it may lead to a buildup of records in minority topics.

Left to its devices, this discussion leads to broader topics, such as queuing and quality of service, and further still, to subjects such as economics, politics, and philosophy, which are firmly outside the scope of this text. The relative tuning of the fetch controls can be likened to the redistribution of wealth. It can only be said that the decision to favour one group over another (in the context of Kafka's topic partitioning, of course) must stem from the non-functional requirements of the application, rather than some hard and fast rule.

The final configuration property pertinent to this discussion is `max.poll.records`, which sets the upper bound on the number of records returned in a single call to `poll()`. Unlike some of the other properties that control the fetch operation on the broker, and akin to the poll timeout, the effects of this property are confined to the client. After receiving the record batches from the brokers, and having unpacked the batches, the consumer will artificially limit the number of records returned. The excluded records will remain in the fetch buffer — to be returned in a subsequent call to `Consumer.poll()`. The default value of `max.poll.records` is `500`.

The original motivation for artificially limiting the number of returned records was largely historical, revolving around the behaviour of the `session.timeout.ms` property at the time. The `max.poll.records` property was introduced in version 0.10.0.0 of Kafka, described in detail in [KIP-41](https://cwiki.apache.org/confluence/display/KAFKA/KIP-41%3A+KafkaConsumer+Max+Records).

The act of polling a Kafka cluster did not just retrieve records — it had the added effect of signalling to the coordinator that the consumer is in a healthy state and able to handle its share of the event stream. Given a combination of a sufficiently large batch and a high time-cost of processing individual records, a consumer's poll loop might have taken longer than the deadline enforced by the `session.timeout.ms` property. When this happened, the coordinator would assume that the consumer had 'given up the ghost', so to speak, and reassign its partitions among the remaining consumers in the encompassing consumer group. In reducing the number of records returned from `poll()`, the application would effectively slacken its processing obligations between successive polls, increasing the likelihood that a cycle would complete before the `session.timeout.ms` deadline elapsed.

A second change was introduced in version 0.10.1.0 and is in effect to this day; the behaviour of polling with respect to consumer liveness was radically altered as part of [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread). The changes saw consumer heartbeating extracted from the `poll()` method into a separate background thread, invoked automatically at an interval not exceeding that of `heartbeat.interval.ms`. Regular polling is still a requisite for proving that a consumer is healthy; however, the poll deadline is now locally enforced on the consumer using the `max.poll.interval.ms` property as the upper bound. If the application fails to poll within this period, the client will simply stop sending heartbeats. This change fixed the root cause of the problem — conflating the cycle time with heartbeating, resulting in either compromising on failure detection time or faulting a consumer prematurely, inadvertently creating a scenario where two consumers might simultaneously handle the same records.

The change to heartbeating markedly improved the situation with respect to timeouts, but it did not address all issues. There is no reliable way for the outgoing consumer to determine that its partitions were revoked — not until the next call to `poll()`. By then, any uncommitted records may have been replayed by the new consumer — resulting in the simultaneous processing of identical records by two consumers — each believing that they 'own' the partitions in question — a highly undesirable scenario in most stream processing applications. The use of a `ConsumerRebalanceListener` does not help in this scenario, as the rebalance callbacks are only invoked from within a call to `poll()`, using the application's polling thread – the same thread that is overwhelmed by the record batch.

To be clear, the improvements introduced in Kafka 0.10.1.0 have not eliminated the need for `max.poll.records`. Particularly when the average time-cost of processing a record is high, limiting the number of in-flight records is still essential to a predictable, time-bounded poll loop. In the absence of this limit, the number of returned records could still backlog the consumer, breaching the deadline set by `max.poll.interval.ms`. The combination of the `max.poll.records` and the more recent `max.poll.interval.ms` settings should be used to properly manage consumer liveness.

For a deeper understanding of how Kafka addresses the liveness and safety properties of the consumer ecosystem, consult Chapter 15: Group Membership and Partition Assignment.

### Group ID

The `group.id` property uniquely identifies the encompassing consumer group, and is integral to the topic subscription mechanism used to distribute partitions among consumers. The `KafkaConsumer` client will use the configured group ID when its `subscribe()` method is invoked. The ID can be up to 255 characters in length, and can include the following characters: `a-z`, `A-Z`, `0-9`, `.` (period), `_` (underscore), and `-` (hyphen). Consumers operating under a consumer group are fully governed by Kafka; aspects such as basic availability, load-balancing, partition exclusivity, and offset persistence are taken care of.

This property does not have a default value. If unset, a _free consumer_ is presumed. Free consumers do not subscribe to a topic; instead, the consuming application is responsible for manually assigning a set of topic-partitions to the consumer, individually specifying the starting offset for each topic-partition pair. Free consumers do not commit their offsets to Kafka; it is up to the application to track the progress of such consumers and persist their state as appropriate, using a data store of their choosing. The concepts of automatic partition assignment, rebalancing, offset persistence, partition exclusivity, consumer heartbeating and failure detection (liveness, in other words), and other so-called 'niceties' accorded to consumer groups cease to exist in this mode.

> The use of the nominal expression 'free consumer' to denote a consumer without an encompassing group is a coined term. It is not part of the standard Kafka nomenclature; indeed, there is no widespread terminology that marks this form of consumer.

### Group instance ID

The `group.instance.id` property specifies a long-term, stable identity for the consumer instance — allowing it to act as a static member of a group. This property is optional; if set, the group instance ID is a non-empty, free-form string that must be unique within the consumer group.

Static group membership is described in detail in Chapter 15: Group Membership and Partition Assignment. The reader is urged to consult this chapter if contemplating the use of static group membership, or mixing static and dynamic membership in the same consumer group.

As an outline, static membership is used in combination with a larger `session.timeout.ms` value to avoid group rebalances caused by transient unavailabilities, such as intermediate failures and process restarts. Static members join a group much like their dynamic counterparts and receive a share of partitions. However, when a static member leaves, the group leader preserves the member's partition assignments, irrespective of whether the departure was planned or unintended. The affected partitions are simply parked; there is no reassignment, and, consequently, the partitions will begin to accumulate lag. Upon its eventual return, the bounced member will resume the processing of its partitions from its last committed point. Static membership aims to lessen the impact of rebalancing at the expense of individual partition availability.

### Heartbeat interval, session timeout, and the maximum poll interval

The `heartbeat.interval.ms`, `session.timeout.ms`, and `max.poll.interval.ms` properties are closely intertwined, collectively controlling Kafka's failure detection behaviour. This behaviour only applies to consumers operating within a group; free consumers are not subject to health checks.

The topic of failure detection and liveness of the consumer ecosystem is covered in Chapter 15: Group Membership and Partition Assignment. The reader will be advised that this topic ranks high on the 'gotcha' spectrum; so much so that the incorrect use of Kafka's failure detection capabilities will jeopardise the correctness of the system, leading to stalled consumers or state corruption.

The following is a highly condensed summary of these properties and their effects.

The `heartbeat.interval.ms` property controls the frequency with which the `KafkaConsumer` client will automatically send heartbeats to the coordinator, indicating that its process is alive and can reach the cluster. On its end, the group coordinator will allow for up to the value of `session.timeout.ms` to receive the heartbeat; failure to receive a heartbeat within the set deadline will result in the forceful expulsion of the consumer from the group, and the reassignment of the consumer's partitions. (This is true for both static and dynamic consumers.)

The `max.poll.interval.ms` stipulates the maximum delay between successive invocations of `poll()`, enforced internally by the `KafkaConsumer`. For dynamic consumers, if the poll-process loop fails to poll in time, the consumer client will cease to send heartbeats and will proactively leave the group — promptly causing a rebalance on the coordinator. For static consumers, a missed deadline will result in the quiescing of heartbeats, but no leave request is sent; it will be up to the coordinator to evict a failed consumer if the latter fails to reappear within the `session.timeout.ms` deadline.

| Property                | Default value      |
| ----------------------- | ------------------ |
| `heartbeat.interval.ms` | 3000 (3 seconds)   |
| `session.timeout.ms`    | 10000 (10 seconds) |
| `max.poll.interval.ms`  | 300000 (5 minutes) |

While the effects of these three properties are abundantly documented and clear, the idiosyncrasies of the associated failure recovery apparatus and the implications of the numerous edge cases remain a mystery to most Kafka practitioners. To avoid getting caught out, the reader is urged to study Chapter 15: Group Membership and Partition Assignment.

### Auto offset reset

The `auto.offset.reset` property stipulates the behaviour of the consumer when no prior committed offsets exist for the partitions that have been assigned to it, or if the specified offsets are invalid.

From the perspective of a consumer acting within an encompassing group, the absence of valid offsets may be observed in three scenarios:

1. When the group is initially formed and the lack of offsets is to be expected. This is the most intuitive and distinguishable scenario and is largely self-explanatory.
2. When an offset for a particular partition has not been committed for a period of time that exceeds the configured retention period of the `__consumer_offsets` topic, and where the most recent offset record has subsequently been truncated.
3. When a committed offset for a partition exists, but the location it points to is no longer valid.

To elaborate on the second scenario: in order to commit offsets, a consumer will send a message to the group coordinator, which will cache the offsets locally and also publish the offsets to an internal topic named `__consumer_offsets`. Other than being an internal topic, there is nothing special about `__consumer_offsets` — it behaves like any other topic in Kafka, meaning that it will eventually start shedding old records. The offsets topic has its retention set to seven days by default. (This is configurable via the `offsets.retention.minutes` broker property.) After this time elapses, the records become eligible for collection.

> Prior to Kafka 2.0.0, the default retention period of `__consumer_offsets` was 24 hours, which confusingly did not align with the default retention period of seven days for all other topics. This used to routinely catch out unsuspecting users; keeping a consumer offline for a day was all it took to lose your offsets. One could wrap up their work on a Friday, come back on the following Monday and occasionally discover that their offsets had been reset. The confusion was exacerbated by the way topic retention works — lapsed records are not immediately purged, they only become candidates for truncation. The actual truncation happens when a log segment file is closed, which cannot be easily predicted as it depends on the amount of data that is written to the topic. [KIP-186](https://cwiki.apache.org/confluence/display/KAFKA/KIP-186%3A+Increase+offsets+retention+default+to+7+days) addressed this issue for release 2.0.0.

The third scenario occurs as a result of routine record truncation, combined with a condition where at least one persisted offset refers to the truncated range. This may happen when the topic in question has shorter retention than the `__consumer_offsets` topic — as such, the committed offsets outlive the data residing at those offsets.

The offset reset consideration is not limited to consumer groups. A _free consumer_ — one that is operating without an encompassing consumer group — can experience an invalid offset during a call to `Consumer.seek()`, if the supplied offset lies outside of the range bounded by the low-water and high-water marks.

Whatever the reason for the missing or invalid offsets, the consumer needs to adequately deal with the situation. The `auto.offset.reset` property lets the consumer select from one of three options:

- **`earliest`**: Reset the consumer to the low-water mark of the topic — the offset of the first retained record for each of the partitions assigned to the consumer where no committed offset exists.
- **`latest`**: Reset the consumer to the high-water mark — the offset immediately following that of the most recently published record for each relevant partition. This is the default option.
- **`none`**: Do not attempt to reset the offsets if any are missing; instead, throw a `NoOffsetForPartitionException`.

> Resetting the offset to `latest`, being the default setting, has the potential to cause havoc, as it runs contrary to Kafka's at-least-once processing tenet. If a consumer group were to lose committed offsets following a period of downtime, the resulting reset would see the consumers' read positions 'jump' instantaneously to the high-water mark, skipping over all records following the last committed point (inclusive of it). Any lag accumulated by the consumer would suddenly disappear. The delivery characteristics for the skipped records would be reduced to 'at most once'. Where consumers request a subscription under a consumer group, it is highly recommended that the default offset scheme is set to `earliest`, thereby maintaining at-least-once semantics for as long as the consumer's true lag does not exceed the retention of the subscribed topic(s).

### Enable auto-commit and the auto-commit interval

The `enable.auto.commit` property controls whether automatic offset committing should be enabled for grouped consumers. The default setting is `true`, which activates the periodic background committing of offsets. This process starts from the point when the application subscribes to one or more topics by invoking one of the overloaded `subscribe()` methods. When enabled, the `auto.commit.interval.ms` property controls the interval of the background auto-commit task, which is set to `5000` (5 seconds) by default. The auto-commit scope encompasses the offsets of the records returned during the most recent call to `poll()`.

The above narrative reflects the official Kafka documentation, but there is more to it. Taking the above for gospel, the more cautious among us might spot a problem. Namely, if auto-commit unconditionally commits offsets every five seconds (or whatever the interval has been set to), what happens to the in-flight records that are yet to be processed? Would they be inadvertently committed, and wouldn't that violate the at-least-once processing semantics?

Indeed, practitioners are mostly divided into two camps: the majority, who have remained oblivious to this concern, and the remaining minority, who have expressed their discomfort with Kafka's default approach. The real answer is somewhat paradoxical. Although it would appear that there is a critical flaw in the consumer's design, and, indeed, the documentation seems to support this theory, the implementation compensates for this in a subtle and surreptitious manner. Whilst the documentation states that a commit will occur in the background at an interval specified by the configuration, the implementation relies on the application's poll-process to initiate the commit from within the `poll()` method, rather than tying an auto-commit action directly to the system time. Furthermore, the auto-commit only occurs if the last auto-commit was longer than `auto.commit.interval.ms` milliseconds ago. By committing from the processing thread, and provided record processing is performed synchronously from the poll-process loop, the `KafkaConsumer` implementation will not commit the offsets of in-flight records, standing by its at-least-once processing vows.

While the above explanation might appear reassuring at first, consider the following: the present behaviour is implementation-specific and unwarranted. It is not stated in the documentation, nor in the Javadocs, nor in the KIPs. As such, there is no commitment, implied or otherwise, on Kafka's maintainers to honour this behaviour. Even a minor release could, in theory, move the auto-commit action from a poll-initiated to a timer-driven model. If the reader is concerned at the prospect of this occurring, it may be prudent to disable the offset auto-commit feature and to always commit the offsets manually using `Consumer.commitAsync()`.

Another implication of the offset auto-commit feature is that it extends the window of uncommitted offsets beyond the set of in-flight records. Whilst this is also true of `Consumer.commitAsync()` to a degree, auto-commit will further compound the delay — up to the value of `auto.commit.interval.ms`. With that in mind, asynchronous manual committing is preferred if the objective is to reduce the persisted offset lag while maintaining a decent performance profile. If, on the other hand, the objective is to curtail the persisted offset lag at any cost, the use of the synchronous `Consumer.commitSync()` method would be most appropriate. The latter may be fitting when the average time-cost of processing a record is high, and so the replaying of records is highly undesirable.

Enabling offset auto-commit may have a minor performance benefit in some cases. If records are 'cheap' to process (in other words, record handling is not resource-intensive), the poll-process cycle will be short, and manual committing will occur frequently. This results in frequent commit messages and increased bandwidth utilisation. By setting a minimum interval between commits, the bandwidth efficiency is improved at the expense of a longer uncommitted window. Of course, a similar effect may be achieved with a simple conditional expression that checks the last commit time and only commits if the offsets are stale. Having committed the offsets, it updates the last commit time (a simple local variable) for the next go-round.

### Partition assignment strategy

The `partition.assignment.strategy` property specifies a comma-separated list of `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor` implementations (in the order of preference) that should be used to orchestrate partition assignment among members of a consumer group. The default value of this property is `org.apache.kafka.clients.consumer.RangeAssignor`, which assigns contiguous partition ranges to the members of the group.

A comprehensive discussion of this topic is presented in Chapter 15: Group Membership and Partition Assignment. In summary, partition assignment occurs on one of the members of the group — the group leader. In order for assignment to proceed, members must agree on a common assignment strategy — constrained by the assignors in the intersection of the (ordered) sets of assignors across all members.

> The reader would have picked up on such terms as 'group leader' and 'group coordinator' throughout the course of this chapter. These refer to different entities. The group leader is a consumer client that is responsible for performing partition assignment. On the other hand, the group coordinator is a broker that arbitrates group membership.

Changing assignors can be tricky, as the group must always agree on at least one assignor. When migrating from one assignor to another, start by specifying both assignors (in either order) in `partition.assignment.strategy` and bouncing consumers until all members have joined the group with both assignors. Then perform the second round of bouncing, removing the outgoing assignor from `partition.assignment.strategy`, leaving only the preferred assignor upon the conclusion of the round. When migrating from the default 'range' assignor, make sure it is added explicitly to the `partition.assignment.strategy` list prior to performing the first round of bounces.

### Transactions

The `isolation.level` property controls the visibility of records written within a pending transaction scope — whereby a transaction has commenced but not yet completed. The default isolation level is `read_uncommitted`, which has the effect of returning all records from `Consumer.poll()`, irrespective of whether they form part of a transaction, and if so, whether the transaction has been committed. Conversely, the `read_committed` isolation mode will return all non-transactional records, as well as those transactional records where the encompassing transaction has successfully been committed.

Because the `read_committed` isolation level conceals any pending records from `poll()`, it will also conceal all following records, irrespective of whether they are part of a transaction — to maintain strict record order from a consumer's perspective.

For a more in-depth discussion on Kafka transactions, the reader may consult Chapter 18: Transactions.

## Admin client configuration

The admin client does not have any unique configuration properties of its own; the properties it employs are shared with the producer and consumer clients.

There is a difference in the construction of a `KafkaAdminClient`, compared to its `KafkaProducer` and `KafkaConsumer` siblings. The latter are instantiated directly using a constructor, as we have seen in the examples thus far. `KafkaAdminClient` does not expose a public constructor. Instead, the `AdminClient` abstract base class offers a static factory method for instantiating a `KafkaAdminClient`.

The `AdminClient` is a more recent addition to the Kafka client family, appearing in version 0.11.0.0, and seems to have taken a different stylistic route compared to its older siblings. (At the time of writing, a static factory method yet to be retrofitted to the `Producer` and `Consumer` interfaces.)

The `AdminClient` interface is rapidly evolving; every significant Kafka release typically adds new capabilities to the admin API. The `AdminClient` interface is marked with the `@InterfaceStability.Evolving` annotation. Its presence means that the API is not guaranteed to maintain backward compatibility across a minor release. From the Javadocs:

```java
/**
 * The administrative client for Kafka, which supports managing
 * and inspecting topics, brokers, configurations and ACLs.
 *
 * ... omitted for brevity ...
 *
 * This client was introduced in 0.11.0.0 and the API is still
 * evolving. We will try to evolve the API in a compatible
 * manner, but we reserve the right to make breaking changes in
 * minor releases, if necessary. We will update the
 * {@code InterfaceStability} annotation and this notice once the
 * API is considered stable.
 */
```

This chapter has taken the reader on a scenic tour of client configuration. There is a lot of it, and almost every setting can materially impact the client. While the number of different settings might appear overwhelming, there is a method to this madness: Kafka caters to varying event processing scenarios, each requiring different client behaviour and potentially satisfying contrasting non-functional demands.

Thorough knowledge of the configuration settings and their implications is essential for both the effective and safe use of Kafka. This is where the official documentation fails its audience in many ways — while the individual properties are documented, the implications of their use and their various behavioural idiosyncrasies are often omitted, leaving the user to fend for themselves. The intent of this chapter was to demystify these properties, giving the reader immense leverage from prior research and analysis; ideally, learning from the mistakes of others, as opposed to their own.

---

# Chapter 11: Robust Configuration

Chapter 10: Client Configuration covered all aspects of client configuration in detail. Among other points, it was mentioned that when a client is instantiated, it verifies that the supplied configuration is valid. In other words, it checks that the given keys correspond to valid configuration property names that are supported in the context of that client type. Failing to meet this requirement will result in a warning message being emitted via the configured logger, but the client will still launch.

Kafka's cavalier approach to configuration raises the following questions: How does one make sure that the supplied configuration is valid _before_ launching the client application? Or must we wait for the application to be deployed before being told that we mucked something up?

## Using constants

The most common source of misconfiguration is a simple typo. Depending on the nature of the misspelled configuration entry, there may be a substantial price to pay for getting it wrong. The example presented in Chapter 10: Client Configuration, involving `max.in.flight.requests.per.connection`, suggests that a mistake may incur the loss of record order under certain circumstances. Relying solely on inspecting log files does not inspire a great deal of confidence — the stakes are too high. There must be a way of nailing the property names; knowing up-front what you give the client will actually be used; and knowing early, before any harm is done.

Kafka does not yet have a satisfactory answer to this, nor is there a KIP in the pipeline that aims to solve this. As a concession, Kafka can meet you halfway with constants:

```java
Map<String, Object> config = Map.of(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
```

In the listing above, property names were replaced with static constants. These constants are stable and will not be changed between release versions. So you can be certain that property names have not been misspelled if they were embedded in code. The last point is crucial, as properties may be loaded from an external source, as it is often the case. Constants will not guard against this.

Constants will also fail to protect the user in those scenarios where one might accidentally add a constant from `ConsumerConfig` into a producer configuration, or vice versa. Admittedly, this is less likely than misspelling a key, but it is still something to be wary of.

## Type-safe configuration

When bootstrapping Kafka client configuration from code or loading configuration artifacts from an external source, the recommended approach is to craft dedicated Java classes with strongly typed values, representing the complete set of configuration items that one can reasonably expect to be supplied when their application is run. The external configuration documents can then be mapped to an object form using a JSON or YAML parser. It is also possible to use a plain `.properties` file if the configuration structure is flat. Alternatively, if using an application framework such as Spring Boot or Micronaut, use the built-in configuration mechanism which supports all three formats.

If it is necessary to support free-form configuration in addition to some number of expected items, add a `Map<String, Object>` attribute to the configuration class. When staging the Kafka configuration, apply the free-form properties first, then apply the expected ones. When applying the expected properties, check if the staging configuration already has a value set — if it does, throw a runtime exception. While this doesn't protect against misspelt property names in the free-form section, it will at least ensure that the expected configuration takes precedence.

For those cases when there is an absolute requirement for the property names to be correct before initialisation, one can take their validation a step further: First, the `java.lang.reflect` package can be used to scan the static constants in `CommonClientConfigs`, `ProducerConfig` or `ConsumerConfig`, then have those constants cross-checked against the user-supplied property names. The validation method will bail with a runtime exception if a given property name could not be resolved among the scanned constants.

The rest of this section will focus on the uncompromising, fully type-safe case. Expect a bit of coding.

All sample code is provided at [github.com/ekoutanov/effectivekafka](https://github.com/ekoutanov/effectivekafka/tree/master/src/main/java/effectivekafka/typesafeproducer), in the `src/main/java/effectivekafka/typesafeproducer` directory.

There are two classes in this example. The first defines a self-validating structure for containing producer client configuration. It has room for both expected properties and a free-form set of custom properties. The listing for `TypesafeProducerConfig` follows.

```java
import static java.util.function.Predicate.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.stream.*;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.serialization.*;

public final class TypesafeProducerConfig {

  public static final class UnsupportedPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    UnsupportedPropertyException(String s) { super(s); }
  }

  public static final class ConflictingPropertyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    ConflictingPropertyException(String s) { super(s); }
  }

  private String bootstrapServers;
  private Class<? extends Serializer<?>> keySerializerClass;
  private Class<? extends Serializer<?>> valueSerializerClass;
  private final Map<String, Object> customEntries = new HashMap<>();

  public TypesafeProducerConfig withBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public TypesafeProducerConfig withKeySerializerClass(Class<? extends Serializer<?>> keySerializerClass) {
    this.keySerializerClass = keySerializerClass;
    return this;
  }

  public TypesafeProducerConfig withValueSerializerClass(Class<? extends Serializer<?>> valueSerializerClass) {
    this.valueSerializerClass = valueSerializerClass;
    return this;
  }

  public TypesafeProducerConfig withCustomEntry(String propertyName, Object value) {
    Objects.requireNonNull(propertyName, "Property name cannot be null");
    customEntries.put(propertyName, value);
    return this;
  }

  public Map<String, Object> mapify() {
    final var stagingConfig = new HashMap<String, Object>();

    if (!customEntries.isEmpty()) {
      final var supportedKeys = scanClassesForPropertyNames(
          SecurityConfig.class, SaslConfigs.class, ProducerConfig.class);
      final var unsupportedKey = customEntries.keySet()
          .stream()
          .filter(not(supportedKeys::contains))
          .findAny();

      if (unsupportedKey.isPresent()) {
        throw new UnsupportedPropertyException(
            "Unsupported property " + unsupportedKey.get());
      }

      stagingConfig.putAll(customEntries);
    }

    Objects.requireNonNull(bootstrapServers, "Bootstrap servers not set");
    tryInsertEntry(stagingConfig, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    Objects.requireNonNull(keySerializerClass, "Key serializer not set");
    tryInsertEntry(stagingConfig, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());

    Objects.requireNonNull(valueSerializerClass, "Value serializer not set");
    tryInsertEntry(stagingConfig, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

    return stagingConfig;
  }

  private static void tryInsertEntry(Map<String, Object> staging, String key, Object value) {
    staging.compute(key, (__key, existingValue) -> {
      if (existingValue == null) {
        return value;
      } else {
        throw new ConflictingPropertyException("Property " + key + " conflicts with an expected property");
      }
    });
  }

  private static Set<String> scanClassesForPropertyNames(Class<?>... classes) {
    return Arrays.stream(classes)
        .map(Class::getFields)
        .flatMap(Arrays::stream)
        .filter(TypesafeProducerConfig::isFieldConstant)
        .filter(TypesafeProducerConfig::isFieldStringType)
        .filter(not(TypesafeProducerConfig::isFieldDoc))
        .map(TypesafeProducerConfig::retrieveField)
        .collect(Collectors.toSet());
  }

  private static boolean isFieldConstant(Field field) {
    return Modifier.isFinal(field.getModifiers()) && Modifier.isStatic(field.getModifiers());
  }

  private static boolean isFieldStringType(Field field) {
    return field.getType().equals(String.class);
  }

  private static boolean isFieldDoc(Field field) {
    return field.getName().endsWith("_DOC");
  }

  private static String retrieveField(Field field) {
    try {
      return (String) field.get(null);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
```

The `TypesafeProducerConfig` class defines a pair of public nested runtime exceptions — `UnsupportedPropertyException` and `ConflictingPropertyException`. The former will be thrown if the user provides a custom property with an unsupported name, trapping those pesky typos. The latter occurs when the custom property conflicts with an expected property. Both are conditions that we ideally would prefer to avoid before initialising the producer client.

Next, we declare our private attributes. This example expects three properties — `bootstrapServers`, `keySerializerClass`, and `valueSerializerClass`. For the serializers, we have taken the extra step of restricting their type to `java.lang.Class<? extends Serializer<?>>`, ensuring that only valid serializer implementations may be assigned. The `customEntries` attribute is responsible for accumulating any free-form entries, supplied in addition to the expected properties.

The `mapify()` method is responsible for converting the stored values into a form suitable for passing to a `KafkaProducer` constructor. It starts by checking if `customEntries` has at least one entry in it. If so, it invokes the `scanClassesForPropertyNames()` method, passing it the class definitions of `ProducerConfig` as well as some common security-related configuration classes. The result will be a set of strings harvested from those classes using reflection. The `ProducerConfig` class already imports the necessary constants from `CommonClientConfigs`, relieving us from having to scan `CommonClientConfigs` explicitly.

The actual implementation of `scanClassesForPropertyNames()` should hopefully be straightforward, requiring basic knowledge of Java 8 and the `java.util.stream` API. Essentially, it enumerates over the public fields, filtering those that happen to be constants (having `final` and `static` modifiers), are of a `java.lang.String` type, and are not suffixed with the string `_DOC`. This set of filters should round up all supported property names. The filtered fields are retrieved and packed into a `java.util.Set`.

> The filter in `scanClassesForPropertyNames()` may also inadvertently include other string constants in the given classes array that happen to match its predicates. Kafka's maintainers haven't consistently differentiated between supported property names and other constants. The majority use the `_CONFIG` suffix to indicate a supported name; however, `ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` has strayed from this convention. Short of specifying an exclusion filter that blacklists known stray constants, there is little we can do about this. A blacklist would create a long-term maintenance headache and is hardly worth the effort. The likelihood of a misspelt user-supplied property name colliding with a stray constant is negligible.

Let's now use our `TypesafeProducerConfig` to configure an actual client:

```java
final var config = new TypesafeProducerConfig()
    .withBootstrapServers("localhost:9092")
    .withKeySerializerClass(StringSerializer.class)
    .withValueSerializerClass(StringSerializer.class)
    .withCustomEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

try (var producer = new KafkaProducer<>(config.mapify())) {
  // do something with producer
}
```

The use of `TypesafeProducerConfig` follows a fluent style of method chaining; the code is compact but readable. But crucially, it is now bullet-proof. You can see it for yourself: feed an invalid property name into `withCustomEntry()` and watch `config.mapify()` bail with a runtime exception, avoiding the initialisation of `KafkaProducer`.

We can also populate a `TypesafeProducerConfig` object from a JSON or YAML configuration file using a parser such as Jackson, available at [github.com/FasterXML/jackson](https://github.com/FasterXML/jackson). Jackson supports a wide range of formats, including JSON, YAML, TOML, and even plain `.properties` files. To map the parsed configuration document to a `TypesafeProducerConfig` instance, Jackson requires either the addition of Jackson-specific annotations to our class or defining a custom deserializer.

Alternatively, if using an application framework such as Spring Boot or Micronaut, we can wire a `TypesafeProducerConfig` object into the framework's native configuration mechanism. This typically requires the addition of setter methods to make the encapsulated attributes writable by the framework. The design of `TypesafeProducerConfig` defers all validation until the `mapify()` method is invoked, thereby remaining agnostic of how its attributes are populated.

> The examples in this book are intentionally decoupled from application frameworks. The intention is to demonstrate the correct and practical uses of Kafka, using the simplest and most succinct examples. These examples are intended to be run as standalone applications using any IDE.

You might have noticed that the example still used a constant to specify the custom property name, in spite of having a reliable mechanism for detecting misspelt names early, before client initialisation. Using constants traps errors at compile-time, which is the holy grail of building robust software.

While the `TypesafeProducerConfig` example solves the validation problem in its current guise, the presented solution is not very reusable. It would take a fair amount of copying and pasting of code to apply this pattern to different configuration classes, even if the variations are minor. With a modicum of refactoring, we can turn the underlying ideas into a generalised model for configuration management.

If you are interested, take a look at [github.com/ekoutanov/effectivekafka](https://github.com/ekoutanov/effectivekafka/tree/master/src/main/java/effectivekafka/config). The `src/main/java/effectivekafka/config` directory contains an example of how this can be achieved. The `AbstractClientConfig` class serves as an abstract base class for a user-defined configuration class. The base class contains the `customEntries` attribute, as well as the validation logic for ensuring the correctness of property names. The deriving class is responsible for providing the expected attributes and the related validation logic. The `mapify()` method lives in the base class, and will invoke the subclass to harvest its expected configuration properties.

This chapter identified the challenges with configuring Kafka clients — namely, Kafka's permissive stance on validating the user-supplied configuration and treating configuration entries as a typeless map of string-based keys to arbitrary values.

The problem of validating configuration for the general case can be solved by creating an intermediate configuration class. This class houses the expected configuration items as type-safe attributes, as well as free-form configuration, which is validated using reflection. A configuration class acts as an intermediate placeholder for configuration entries, enabling the application to proactively validate the client configuration before instantiating the client, in some cases using nothing more than compile-time type safety.

---

# Chapter 12: Batching and Compression

Chapter 10: Client Configuration mostly attended to the aspects of client configuration related to functionality and safety, deliberately sidestepping any serious discussions on performance. The intent of this chapter is to focus on one specific area of performance optimisation — batching and compression. The two are related, collectively bearing a significant impact on the performance of an event streaming system.

## Comparing disk and network I/O

Kafka utilises a segmented, append-only log, largely limiting itself to sequential I/O for both reads and writes, which is fast across a wide variety of storage media. There is a wide misconception that disks are slow; however, the performance of storage media (particularly rotating media) is greatly dependent on access patterns. The performance of random I/O on a typical 7,200 RPM SATA disk is between three and four orders of magnitude slower than sequential I/O. Furthermore, a modern operating system provides read-ahead and write-behind techniques that prefetch data in large block multiples and group smaller logical writes into large physical writes. Because of this, the difference between sequential I/O and random I/O is still evident in flash and other forms of solid-state non-volatile media, although the effects are less dramatic compared to rotating media.

Sequential I/O is comparable to the peak performance of network I/O. Furthermore, disk I/O is local to the host, whereas network I/O is shared. In practice, this means that a well-designed log-structured persistence layer will keep up with the network traffic. In fact, often the bottleneck with Kafka's performance isn't the disk, but the network. Stated bluntly, the network fabric will run out of steam before the broker.

## Producer record batching

To counteract the limitations of the network, Kafka clients will batch multiple records together before sending them over the network. This is independent of, and in addition to, the low-level batching provided by the OS at the TCP socket layer. Batching of records amortises the overhead of the network round-trip, using larger packets and improving bandwidth efficiency.

Batching in Kafka can act end-to-end. The producer client uses an accumulator buffer for staging records prior to forwarding them to the leader broker. Once the records are batched, the broker will (in most cases) persist them as-is, without unpacking the batch or performing any intermediate manipulations of the stored records. This is carried through to the consumer. When polling for records, the consumer is served batches of records by the broker — the same batches that were originally published. There are cases where a batch is not end-to-end; for example, when a broker is instructed to apply a compression scheme that differs from the producer. More on that later.

Being a largely end-to-end concern, batching is controlled by the producer. The `batch.size` and `linger.ms` properties collectively limit the extent to which the producer will attempt to batch queued records in order to maximise the outgoing transmission efficiency. The default values of `batch.size` and `linger.ms` are `16384` (number of bytes) and `0` (milliseconds), respectively.

The producer combines any records that arrive between request transmissions into a single batched request. Normally this only occurs under load, when records arrive faster than they can be transmitted. In some circumstances, the client may want to reduce the number of requests even under moderate load. The `linger.ms` setting accomplishes this by adding a small amount of artificial delay — rather than immediately sending a record the moment it is enqueued, the producer will wait for up to a set delay to allow other records to accumulate in a batch, maximising the amount of data that can be transmitted in one go. Although records may be allowed to linger for up to the duration specified in `linger.ms`, the `batch.size` property will have an overriding effect, dispatching the batch once it reaches the set maximum size.

While the `linger.ms` property only comes into the picture when the producer is lightly loaded, the `batch.size` property is continually in effect, acting as the overarching and unremitting limiter of the staged batch.

The `linger.ms` setting is often likened to Nagle's Algorithm in TCP, as they both aim to improve the efficiency of network transmission by combining small and intermittent outgoing messages and sending them at once. The comparison was originally made by the Apache Kafka design team. It has since found its way into the official documentation and appears to reverberate strongly within the user community.

While the similarities are superficial, there are two crucial differences. Firstly, Nagle's Algorithm does not indiscriminately buffer data on the basis of its apparent intermittence. It only takes effect when there is at least one outstanding packet that is yet to be acknowledged by the receiver. Secondly, Nagle's Algorithm does not impose an artificial time limit on the delay. Data will be buffered until a full packet is formed, or the ACK for the previous packet is received, whichever occurs first. Combined, the two instruments make the algorithm self-regulating; rather than relying on arbitrary, user-defined linger times, the algorithm buffers data based on the network's observed performance. Free-flowing networks with frequent ACKs result in lighter buffering and reduced transmission latency. As congestion increases, the buffering becomes more pronounced, improving transmission efficiency at the expense of latency, and helps avoid a congestive collapse.

> **Congestive collapse** is a phenomenon that occurs when a network is overwhelmed by a high packet rate, usually at known choke points, leading to increased failures (packet loss and timeouts) and a corresponding escalation of retries. This creates a perpetuating feedback loop that degenerates to a parasitic stable state where the traffic demand is high, but there is little useful throughput available.

The batching algorithm used by Kafka is crude by comparison. It requires careful tuning and imposes a fixed penalty on intermittent publishing patterns regardless of the network's performance or the observed latency. This does not necessarily render it ineffective. On the contrary, the batching algorithm can be very effective over high-latency networks. Its main issue is the lack of adaptability, making it suboptimal in dynamic network climates or in the face of varying cluster performance — both factors affecting publishing latency. Kafka places the onus of tuning the algorithm parameters on the user, requiring careful and extensive experimentation to empirically arrive at the optimal set of parameters for a given network and cluster profile. These parameters must also be periodically revised to ensure their continued viability.

Even with the default `linger.ms` value of `0`, the producer will still allow for some buffering due to the asynchronous nature of the transmission: calling `send()` serializes the record, assigns a partition number, and places the serialized contents into the accumulator buffer, but does not transmit it. The actual communications are handled by a background I/O thread. So if `send()` is called multiple times in rapid succession, at least some of these records will likely be batched.

Due to the diminishing returns exhibited with larger batch sizes and the lack of self-regulation in Kafka's batching algorithm, it may be prudent to err on the side of smaller values of `linger.ms` initially — from zero to several milliseconds. The non-functional requirements of the overall system should also specify the tolerance for latency, which ought to be honoured over any gains in network efficiency or storage savings on the brokers. The extent of batching may be increased further if the network is becoming a genuine bottleneck; however, such changes should be temporary — the focus should be on addressing the root cause.

## Compression

The effectiveness of batching increases substantially when complemented by record compression. As compression operates on an entire batch, the resulting compression ratios increase with the batch size. The effects of compression are particularly pronounced when using text-based encodings such as JSON, where the records exhibit low information entropy. For JSON, compression ratios ranging from 5x to 7x are not unusual, which makes enabling compression a no-brainer. Furthermore, record batching and compression are largely done as a client-side operation, which transfers the load onto the client and has a positive effect not only on the network bandwidth, but also on the brokers' disk I/O and storage utilisation.

The biggest gains will be felt when starting from small batches. As the batch size increases, the laws of diminishing returns take effect — an increase in the batch size will yield a proportionally smaller gain in compression ratio. The incremental reduction in the number of packets used, and hence the transmission efficiency, will also be less noticeable with higher batch sizes.

The `compression.type` client configuration property controls the algorithm that the producer will use to compress record batches before forwarding them on to the partition leaders. The valid values are:

- **`none`**: Compression is disabled. This is the default setting.
- **`gzip`**: Use the GNU Gzip algorithm — released in 1992 as a free substitute for the proprietary `compress` program used by early UNIX systems.
- **`snappy`**: Use Google's Snappy compression format — optimised for throughput at the expense of compression ratios.
- **`lz4`**: Use the LZ4 algorithm — also optimised for throughput, most notably for the speed of decompression.
- **`zstd`**: Use Facebook's ZStandard — a newer algorithm introduced in Kafka 2.1.0, intended to achieve an effective balance between throughput and compression ratios.

Kafka compression applies to an entire record batch, which as we know can be end-to-end — depending on the settings on the broker. Specifically, when the broker is configured to accept the producer's preferred compression scheme, it will not interfere with the contents of the batch. The batch flows from the producer to the broker, is persisted across multiple replicas, and is eventually served to one or more consumers — all as a single, indivisible chunk. The broker simply acts as a relaying party — it does not decompress and re-compress the record as part of its role. Each chunk has a header that notes the algorithm that was used during compression, allowing the consumers to apply the same when unpacking the chunk.

The `compression.type` broker property applies to all topics by default, overriding the producer property. In addition, it is possible to configure individual topics by using the `kafka-configs.sh` CLI to specify a dynamic configuration for the `topics` entity type.

On the flip side, end-to-end compression has a subtle drawback, which can catch out unsuspecting users. Because the broker is unable to mediate the interchange format, the result is a latent coupling between the producer's capabilities and that of the consumer clients. Although Kafka strives to maintain binary protocol compatibility between minor releases, this promise does not cover end-to-end contracts, such as compression and checksumming. As such, the producer application must use a compression format that is compatible with the oldest consumer version.

> The introduction of ZStandard is a good example of a breaking change, and may be considered as a 'gotcha' depending on your client ecosystem. When operating a mixture of 2.1.0+ and pre-2.1.0 consumers, the use of `compression.type=zstd` on a producer will render the records unreadable for the older clients, resulting in an `UNSUPPORTED_COMPRESSION_TYPE` error. The correct way to enable ZStandard is to upgrade all consumers first, and only when the last pre-2.1.0 consumer has been retired, allow the use of `compression.type=zstd`. Alternatively, one can enable re-compression on the broker to maintain compatibility with older clients; however, this leads to increased resource utilisation on the broker.

End-to-end compression has a profoundly positive impact on performance. Compression is a processor-intensive operation and consumes additional memory, particularly during the encoding phase. (Comparatively, decoding a compressed stream is cheaper; the difference may be an order of magnitude in extreme cases.) By eliminating the broker from the equation, the cost of compression is absorbed by the clients. The distribution of load to the periphery dovetails into Kafka's broader scaling strategy — it is typically much easier to scale the clients than the broker, at least for well-architected, stateless applications.

Kafka additionally offers compression at the broker level, for those scenarios where it is necessary to change the compression scheme. This is controlled by the `compression.type` broker property. Its default value is `producer`, meaning that the broker will revert to the producer-assigned compression scheme; in other words, the broker will not meddle the batch. Alternatively, the value of `compression.type` may be set to one of the supported compression schemes (as per the producer-side property). The only difference is when disabling compression: the producer property accepts `none`, whereas the broker property accepts `uncompressed`.

While broker-level configuration can provide for more fine-grained control of the compression scheme, its main drawback is the increased CPU utilisation and the forfeiting of the zero-copy optimisation, as the batches are no longer end-to-end.

> **Zero-copy** describes computer operations in which the CPU does not perform the task of copying data from one memory area to another. In a typical I/O scenario, the transfer of data from a network socket to a storage device occurs without the involvement of the CPU and with a reduced number of context switches between the kernel and user mode.

The use of compression has no functional effect on the system. Its effect on bandwidth utilisation, disk space utilisation, and broker I/O are, in some cases, astounding. As a performance optimisation, and depending on the type of data transmitted, the effect of compression can be so profound that it thwarts any potential philosophical debates over the viability of 'premature' optimisation.

Compression algorithms achieve a reduction in the encoded size relative to the uncompressed original by replacing repeated occurrences of data with references to a single copy of that data existing earlier in the uncompressed data stream. For compression to be effective, the data must contain a large amount of repetition and be of sufficient size so as to warrant any structural overheads, such as the introduction of a dictionary. Text formats such as JSON tend to be highly compressible as they are verbose by nature and contain repeated character sequences that carry no informational content. They also fail to take advantage of the full eight bits that each byte can theoretically accommodate, instead representing characters with seven of the lower order bits.

Binary encodings tend to exhibit more informational density, but may still be highly compressible, depending on their internal structure. Binary streams containing digital media — for example, images, audio or video — are high-entropy sources and are virtually incompressible.

> **Information entropy** is the measure of the informational content conveyed by an element in a stream. It is determined by the likelihood of predicting the value of an element in a stream based on the observations of prior elements, with the resulting score varying between zero (no entropy, perfectly predictable) and one (highest entropy, completely unpredictable). In the context of a data record, an element might be an individual bit or a byte in the record.

The easier it is to predict the value of the next byte, the less new information it carries. As an example of a low entropy source, consider a formatted JSON document. Having observed a newline character, it is extremely likely that the next element is a whitespace character, with the next likely candidate being a double quote, followed in relative likelihood by a closing brace. Compare this to a truly random sequence of bytes. The likelihood of predicting the next byte is equivalent to chance; this is an example of maximum entropy. Compressing this type of data will only lead to an increase in the output size as the overheads of the compression algorithm will be added into the output, not offset by any gains in entropy.

Without delving into a harrowing analysis of the different compression schemes, the recommendation is to always enable compression for text encodings as well as binary data (unless the latter is known to contain a high information entropy payload). In some cases, the baseline impact of enabling compression may be so substantial that the choice of the compression algorithm hardly matters. In other cases, the choice of the algorithm may materially impact the overall performance, and a more careful selection is warranted.

As a rule of thumb:

- Use **LZ4** when dealing with legacy consumers, transmitting over a network that offers capacity in excess of your peak uncompressed data needs. In other words, the network is able to sustain your traffic flow even without compression.
- If the network has been identified as the bottleneck, consider switching to **Gzip** and also increasing `batch.size` and `linger.ms` to increase the size of transmitted batches to maximise the effectiveness of compression at the expense of latency.
- If all consumers are at a version equal to or greater than 2.1.0, your choices are basically **LZ4** and **ZStandard**. Use LZ4 for low-overhead compression. When the network becomes the bottleneck, consider ZStandard, as it is able to achieve similar compression ratios to Gzip at a fraction of the compression and decompression time. You may also need to increase `batch.size` and `linger.ms` to maximise compression effectiveness.

The guidelines above should not be taken to mean that LZ4 always outperforms Snappy or that ZStandard should always be preferred over Gzip. When undertaking serious performance tuning, you should carefully consider the shape of your data and conduct studies using synthetic records that are representative of the real thing, or better still, using historical data if this is an option.

When benchmarking the different compression schemes, you should also measure the CPU and memory utilisation of the producer and consumer clients, comparing these to the baseline case (when compression is disabled). In some cases you may find that Snappy or Gzip indeed offer a better compromise. The guidelines presented here should be used as the starting point, particularly when one's copious free time is prioritised towards dealing with the matters of building software, over conducting large-scale performance trials.

This chapter has given the reader an insight into Kafka's performance 'secret sauce' — namely, the use of log-structured persistence to limit access patterns to sequential reads and writes. The implication: a blazingly fast disk I/O subsystem that can outperform the network fabric, requiring further client-side optimisations to bring the two into parity.

We looked at two controls available on the producer client — batching and compression. The potential performance impacts of these controls are significant, particularly in the areas of throughput and latency. Getting them right could entail significant gains with relatively little effort.

---

# Chapter 13: Replication and Acknowledgements

Fundamentally, Apache Kafka is a distributed log-centric data store. Data is written across multiple nodes in a cluster and may be subject to a range of contingencies — disk failures, intermittent timeouts, process crashes, and network partitions. How Kafka behaves in the face of a contingency and the effect this has on the published data should be of material concern to the designer of an event-driven system.

This chapter explores one of the more nuanced features of Kafka — its replication protocol.

## Replication basics

The deliberate decisions made during the design of Kafka ensure that data written to the cluster will be both durable and available — meaning that it will survive failures of broker nodes and will be accessible to clients. The replication protocol is the specific mechanism by which this is achieved.

The fundamental unit of streaming in Kafka is a partition. For all intents and purposes, a partition is a replicated log. The basic premise of a replicated log is straightforward: data is written to multiple replicas so that the failure of one replica does not entail the loss of data. Furthermore, replicas must agree among themselves with respect to the contents of the replicated log. Broadly speaking, this notional agreement among the replicas is referred to as distributed consensus.

Kafka follows a leader-follower model — a single leader is assigned by the cluster controller to take absolute mastership of the partition, with zero or more followers that tail the data written by the leader in near real-time. Replication in Kafka is asynchronous: replicas lag behind the leader, converging on its state when the traffic flow quiesces. Consensus is formed by ensuring that only one party administers changes to the log; all other parties implicitly agree by unconditionally replicating all changes from the leader, achieving sequential consistency.

### In-Sync Replicas (ISR)

To lighten the burden of slow replicas, Kafka introduces the concept of **In-Sync Replicas (ISR)**. This is a dynamically allocated set of replicas that can demonstrably keep pace with the leader. The leader is also included in the ISR. Slow replicas are automatically removed from the ISR set.

A partition that has had at least one replica removed from the ISR is said to be _under-replicated_. Instead of requiring the leader to garner acknowledgements from all follower replicas, a durable write only requires that acknowledgements are received from those replicas in the ISR. The lower bound on the size of the ISR is specified by the `min.insync.replicas` configuration property on the broker.

## Leader election

Only members of the ISR are eligible for leader election. Kafka’s replication protocol is generally asynchronous, meaning a replica in the ISR is not guaranteed to have all records that were written by the outgoing leader, only those confirmed as durably persisted. When selecting the new leader, Kafka will favour the follower with the highest log end offset, recovering as much of the unacknowledged data as possible.

Kafka provides two options when all in-sync replicas are lost: either wait until an in-sync replica is restored, or perform **unclean leader election** (enabled by setting `unclean.leader.election.enable` to `true`). Unclean leader election allows replicas that were not in the ISR at the time of failure to take over partition leadership, trading consistency for availability.

## Setting the initial replication factor

The replication factor can be initially assigned when creating a topic using the Kafka Admin API or the `--replication-factor` flag in the `kafka-topics.sh` CLI tool. If unspecified, it defaults to `1`. Attempting to create a topic with a replication factor greater than the size of the cluster results in an `InvalidReplicationFactorException`.

## Changing the replication factor

Changing the replication factor is a semi-manual operation which entails specifying a new set of replicas for each partition via a reassignment JSON file.

```json
{
  "version": 1,
  "partitions": [
    { "topic": "growth-plan", "partition": 0, "replicas": [1002, 1001] },
    { "topic": "growth-plan", "partition": 1, "replicas": [1001, 1002] }
  ]
}
```

Apply the reassignment file using the `kafka-reassign-partitions.sh` tool:

```bash
$KAFKA_HOME/bin/kafka-reassign-partitions.sh \
  --zookeeper localhost:2181 \
  --reassignment-json-file alter-replicas.json --execute
```

To throttle the replication process, use the `--throttle` flag to cap the bandwidth in bytes per second.

## Decommissioning broker nodes

The `kafka-reassign-partitions.sh` tool is designed to administer arbitrary changes to the replication topology, making it ideal for moving partitions off a broker node before it can be safely decommissioned. Create a reassignment file covering all partitions for which the outgoing broker is a replica, substituting the outgoing broker ID with a remaining broker, and execute the reassignment.

## Acknowledgements

The `acks` property stipulates the number of acknowledgements the producer requires the leader to have received before considering a request complete.

### No acknowledgements (`acks=0`)

The producer does not wait for any acknowledgement. This provides the weakest durability guarantee and is suited for scenarios where losing a few records (e.g., telemetry data) is acceptable.

### One acknowledgement (`acks=1`)

The client waits until the partition leader queues the write to its local log. The broker does not invoke `fsync` for each written record, meaning the broker could acknowledge the write and fail immediately thereafter, losing the chunk before it is replicated.

### All acknowledgements (`acks=all` or `acks=-1`)

The client waits until the partition leader has gathered acknowledgements from the complete set of in-sync replicas. This is the highest guarantee on offer, ensuring that the record is durably persisted for as long as one in-sync replica remains.

> **Gotcha:** The default setting of `acks` is `1` when idempotence is disabled, and the default value of `min.insync.replicas` is `1`. For strict durability, you must explicitly configure `acks=all` and `min.insync.replicas=2` (with a replication factor of 3).

---

# Chapter 14: Data Retention

Given that infinite storage is yet to be invented, what happens to all those records stored in Kafka? This chapter focuses on the conditions under which data is removed and the precise behaviour of Kafka in that regard.

## Kafka storage internals

### Organisation of log data

The log files for each replicated partition are stored in a dedicated subdirectory of `log.dirs`. Kafka does not store its partition log in a single, contiguous file. Instead, logs are broken down into discrete chunks called **log segments**. Each segment is named in accordance with the offset of the first record it contains (the _base offset_).

### Indexes

Because records are of variable size and persisted in batches, a record cannot be trivially located by its offset using the `.log` file alone. The `.index` file acts as a sorted map of record offsets to physical locations. Indexes are memory-mapped files. Additionally, the `.timeindex` file maps the millisecond-precise timestamp of each record to its location, allowing Kafka to locate records based on time.

### Rotation of log segments

A replica maintains a single _active_ log segment for every partition. Writes are appended to the active segment. The segment is rolled over when it breaches constraints defined by:

- `log.segment.bytes` (default 1 GiB)
- `log.roll.hours` / `log.roll.ms` (default 1 week)

## Deletion

The `delete` cleanup policy operates at the granularity of log segments. A background process evaluates inactive log segments for deletion based on:

- `log.retention.bytes` (maximum log size)
- `log.retention.hours` / `minutes` / `ms` (default 1 week)

Because deletion operates on whole segments, an active segment will not be deleted until it is rolled over.

## Compaction

### Use cases behind compaction

Log compaction provides a finer-grained, per-record culling strategy. It selectively prunes records where a more recent update exists for the same key. This guarantees that the log retains at least one record representing the most recent snapshot of an entity, making Kafka viable as a primary event store for event sourcing (unimodal processing).

### Overwriting and deleting records

Compaction is activated by adding `compact` to `log.cleanup.policy`. To delete an entity, the producer publishes a **tombstone** — a record with the same key but a `null` value. Tombstones trigger the purging of older records for that key but are themselves retained for a period (`delete.retention.ms`, default 24 hours) to ensure lagging consumers receive the deletion signal.

### Behind the scenes

Compaction is driven by the `log.cleaner.threads`. It prioritises logs with the highest "dirty ratio" (the ratio of uncompacted head size to overall length). Compaction preserves the order and original offsets of surviving records, creating "holes" in the offset sequence that consumers must disregard.

## Combining compaction with deletion

It is possible to assign both `delete` and `compact` policies. This "hybrid" model is useful in change data capture scenarios with fast-moving data, where compaction accelerates processing by removing obsolete records, while deletion bounds the overall size of the topic. The internal `__consumer_offsets` topic uses this hybrid approach.

---

# Chapter 15: Group Membership and Partition Assignment

Partitions in a topic are allocated approximately evenly among the live members of a consumer group. This chapter explores Kafka’s group membership protocol and the mechanisms ensuring members make progress and records are processed exclusively.

## Group membership basics

### Establishing group membership

The protocol is divided into two phases: **group membership** and **state synchronisation**.

1. Consumers send a `JoinGroupRequest` to the group coordinator (a broker implicitly selected from the partition leaders of the internal `__consumer_offsets` topic).
2. The coordinator selects a group leader and replies with the member IDs.

### State synchronisation

The group leader delegates to a `ConsumerPartitionAssignor` to perform partition assignment, communicating the outcome to the coordinator via a `SyncGroupRequest`. The coordinator then replies to all members with their individual assignments.

### Delayed rebalance

To prevent a "swarm" effect of unnecessary rebalances when consumers join rapidly, the `group.initial.rebalance.delay.ms` property (default 3 seconds) allows additional time for consumers to join before the coordinator sends `JoinGroupResponse` messages.

### State synchronisation barrier

To prevent time-overlap between the revocation of a partition from an outgoing consumer and its assignment to a new one, Kafka uses the `ConsumerRebalanceListener` callback. The `onPartitionsRevoked()` method acts as a global barrier — creating a "stop-the-world" pause where no consumer processes records until all consumers have handled the revocation.

### Incremental cooperative rebalancing

Introduced in Kafka 2.4.0 (KIP-429), this protocol avoids the pessimistic "stop-the-world" pause of eager rebalancing. It separates revocations from assignments by exactly one join-rebalance round, communicating exact revocations to consumers and drastically reducing cleanup work and pause times.

### Static membership

Static group membership associates members with a long-term, stable identity via the `group.instance.id` property. Members can leave and rejoin without forfeiting their partition assignment or causing a rebalance, provided they return within the `session.timeout.ms` deadline. This is highly beneficial for containerised environments (e.g., Kubernetes) where pods may be restarted.

## Liveness and safety

- **Liveness** ensures the system eventually makes progress (e.g., identifying failed consumers and rebalancing partitions).
- **Safety** ensures critical invariants are never violated (e.g., preserving record order and ensuring a partition is processed by at most one consumer in a group).

Kafka satisfies liveness via:

1. **Availability checks:** Heartbeats sent at `heartbeat.interval.ms` must be received within `session.timeout.ms`.
2. **Progress checks:** `KafkaConsumer.poll()` must be invoked within `max.poll.interval.ms` (default 5 minutes).

### Dealing with failures

If a consumer blocks indefinitely on a downstream dependency, it will breach `max.poll.interval.ms`. Strategies to handle this include:

1. Setting an absurdly large `max.poll.interval.ms` (disabling the progress check).
2. Allowing Kafka to detect the failure and rebalance the group.
3. Voluntarily relinquishing the subscription before the deadline.
4. Implementing a record-level deadline and re-queuing the record.
5. Skipping the record and sending it to a dead-letter topic.

### Dealing with partition exclusivity

Kafka guarantees exclusive partition _assignment_, but not exclusive _processing_. If a consumer blocks and is evicted, it might continue processing records concurrently with the new assignee (a "zombie" process).
To prevent this, applications must employ:

1. Strict adherence to `max.poll.interval.ms`.
2. External fencing mechanisms (e.g., Distributed Lock Managers or database transactions).
3. Process-level fencing (e.g., Kubernetes restarting the blocked pod).

## Partition assignment strategy

Partition assignment is performed by the group leader using a `ConsumerPartitionAssignor`.

### Built-in assignors

1. **RangeAssignor (Default):** Assigns contiguous partition ranges per topic. Suffers from uneven assignments when consumers subscribe to multiple topics with varying partition counts.
2. **RoundRobinAssignor:** Aggregates all partitions across all topics and assigns them in a round-robin fashion. Provides ideal load distribution for homogeneous subscriptions.
3. **StickyAssignor:** Maintains an evenly-balanced distribution while preserving prior assignments as much as possible, minimising partition movement during rebalances.
4. **CooperativeStickyAssignor:** A variant of the sticky assignor that utilises the newer incremental cooperative rebalance protocol to reduce the stop-the-world pause.

### Upgrading assignors

Changing an assignor requires a **two-round bounce** to prevent `INCONSISTENT_GROUP_PROTOCOL` errors.

1. Update the `partition.assignment.strategy` to include _both_ the old and new assignors, and bounce all consumers.
2. Update the configuration to include _only_ the new assignor, and bounce all consumers again.

---

# Chapter 16: Security

With the phenomenal level of interconnectedness prevalent in the modern world, opportunities arise not just in legitimate commercial enterprises, but also in the more clandestine establishments. The topic of conversation is cybercrime and information security. Kafka is a persistent data store that potentially contains sensitive organizational data. As such, it is essential that particular attention is paid to the security of Kafka deployments.

## State of security in Kafka

Let us start by making one thing abundantly clear: **Kafka is not secure by default.**

The default Kafka security profile has several notable shortfalls:

- Any client can establish a connection to a ZooKeeper or Kafka node (including diagnostic ports like JMX).
- Connections to Kafka brokers are unencrypted (cleartext TCP).
- Connections to Kafka brokers are unauthenticated.
- No authorization controls are in place (default policy is allow-all).

## Target state security

Before embarking on the journey of progressively hardening the cluster, it is worthwhile to model an ideal target state:

1. **Minimise the attack surface:** Limit access to the cluster at the lowest possible level using network policies.
2. **Ensure traffic confidentiality:** Encrypt traffic flowing in and out of the cluster using TLS/SSL.
3. **Know the client:** Authenticate clients to attest their identity.
4. **Limit access to essential data and functionality:** Authorize clients using Access Control Lists (ACLs) to enforce the Principle of Least Privilege (PoLP).

## Network traffic policy

No control is more effective than a network-level traffic policy (a firewall). At a minimum, the network should be partitioned into static segments:

- **ZooKeeper ensemble:** Heavily isolated network segment.
- **Broker network:** Separated from ZooKeeper by a stateful firewall.
- **Internal client network:** For producer, consumer, and admin clients.
- **External network:** Everything outside the perimeter (e.g., the public Internet).

Edge locations that require direct connectivity into the core client network are best accommodated using secure virtual networks, such as VPNs.

## Confidentiality

Kafka supports Transport Layer Security (TLS) for encrypting traffic. In Kafka terminology, this is referred to as **SSL**.

### Client-to-broker encryption

To enable SSL, you must generate a private key and certificate for each broker, signed by a Certificate Authority (CA).

**1. Generate the private key:**

```bash
keytool -keystore server.keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA
```

**2. Create a CA:**

```bash
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
```

**3. Sign the broker certificate:**

```bash
keytool -keystore server.keystore.jks -alias localhost -certreq -file cert-req
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-req -out cert-signed -days 365 -CAcreateserial
```

**4. Import certificates into truststores and keystores:**

```bash
keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore server.keystore.jks -alias localhost -import -file cert-signed
```

**5. Configure the broker (`server.properties`):**

```properties
listeners=PLAINTEXT://:9092,SSL://:9093
advertised.listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093
ssl.keystore.location=/path/to/server.keystore.jks
ssl.keystore.password=secret
ssl.key.password=secret
ssl.truststore.location=/path/to/server.truststore.jks
ssl.truststore.password=secret
```

Once configured, clients must specify `security.protocol=SSL` and supply the truststore to verify the broker. The `ssl.endpoint.identification.algorithm` property (default `https`) enables hostname verification against the certificate. Set it to an empty string to disable.

### Interbroker encryption

To encrypt interbroker communications, simply change the interbroker listener to SSL:

```properties
inter.broker.listener.name=SSL
```

Once verified, the recommended next step is to disable the cleartext listener entirely, updating all brokers and clients in the interim to use SSL, then remove `PLAINTEXT` from `listeners` and `advertised.listeners`.

### Broker-to-ZooKeeper encryption

The current version of Kafka (2.4.0 at the time of writing) does not support encrypted broker-to-ZooKeeper traffic natively (KIP-513 targets release 2.5.0). For security-minded deployments, consider tunnelling the connection over a VPN or using a service mesh proxy capable of transparently initiating TLS connections.

### Encryption at rest

Kafka does not have native facilities for enabling encrypted storage of record data. One must resort to:

- **Full disk encryption** — protects data when disks are detached from the host.
- **Filesystem-level encryption** — similar protection at a finer granularity.
- **End-to-end encryption** — encrypting the payload on the producer and decrypting on the consumer. This protects against embedded threats on the broker host.

When using end-to-end encryption, the information entropy of record batches approaches unity, so compression should be disabled as it will only burn CPU cycles without decreasing payload size. Note that end-to-end encryption does not eliminate the need for TLS — SSL still protects metadata, record headers, offsets, group membership, and guards against man-in-the-middle attacks.

## Authentication

Kafka supports several modes for attesting the identity of connected clients.

### Mutual TLS (mTLS)

Mutual TLS (also known as client-side X.509 authentication or two-way SSL/TLS) utilises the same principle of certificate signing as conventional TLS, but in the opposite direction. Each client has a dedicated certificate signed by a trusted CA.

To enable client authentication, set `ssl.client.auth` in `server.properties`:

- **`none`** (default): Client authentication is disabled.
- **`requested`**: Client may optionally authenticate; a "halfway house" for gradual migration.
- **`required`**: The broker mandates client-side authentication; connections without a valid certificate are rejected.

Full worked example for enabling mTLS:

```bash
# Generate a private key for the client
keytool -keystore client.keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA

# Sign the client certificate
keytool -keystore client.keystore.jks -alias localhost -certreq -file client-cert-req
openssl x509 -req -CA ca-cert -CAkey ca-key -in client-cert-req -out client-cert-signed -days 365 -CAcreateserial

# Import CA and signed certificate into client keystore
keytool -keystore client.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore client.keystore.jks -alias localhost -import -file client-cert-signed
```

Configure the broker:

```properties
ssl.client.auth=required
```

On the client side, add the keystore configuration:

```java
config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "client.keystore.jks");
config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "secret");
config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "secret");
```

The user principal is taken from the CN attribute of the certificate. This can be customised via `ssl.principal.mapping.rules` in the format `RULE:pattern/replacement/[LU]`.

**Important limitations of mTLS:**

- **Impersonation risk:** If the signing process is not tightly controlled, one client could request a certificate with another client's CN, impersonating it. The relationship between client and CA must be individually authenticated.
- **No certificate revocation:** This has been an open issue since May 2016. If a private key is compromised, the only workaround is to remove privileges from the affected principal and rotate usernames — or redeploy a new CA and re-sign all legitimate certificates. When using mTLS across multiple clusters, segregate trust chains so each cluster trusts only its own intermediate CA.
- **Incompatibility with application-level auth:** Despite operating at different OSI layers, two-way SSL cannot be used in conjunction with SASL authentication — it is either one or the other.

### SASL (Simple Authentication and Security Layer)

Kafka supports SASL — an extensible framework for embedding authentication in application protocols, decoupling authentication concerns from application protocols. SASL is rarely used on its own; the most common deployment model pairs it with TLS.

#### SASL/GSSAPI (Kerberos)

Kafka supports GSSAPI, commonly associated with Kerberos (its dominant implementation). Kerberos and Active Directory work best for interactive users in a corporate setting, but Kafka clients are rarely individuals — they are applications using service accounts. Centralised authentication for service accounts is challenging because backend applications consume numerous disparate resources (Kafka, Postgres, Redis, third-party APIs), not all of which support Kerberos. The prevalent industry trend is migration away from centralised directories towards **centralised secrets management systems** — managing credentials rather than principals.

This chapter does not provide a full Kerberos worked example due to its complexity; it is adequately documented at kafka.apache.org/documentation.

#### SASL/SCRAM

SCRAM (Salted Challenge Response Authentication Mechanism) fulfills authentication without the explicit transfer of credentials. It is a protocol designed to authenticate without sending the password over the wire.

SCRAM is bidirectional: not only must the client prove to the broker that it has the password, but the broker must also prove that it knew the password at some point in time. This is asymmetric — the client proves **present knowledge**, while the broker proves **past knowledge**. This relieves the broker from persisting the password verbatim; instead, it stores irreversible derivations (salts and hashes).

Kafka supports **SCRAM-SHA-256** and **SCRAM-SHA-512**. Both are very strong; SHA-512 offers better collision resistance and is optimised for 64-bit processors, while SHA-256 is more performant on 32-bit. SHA-256 remains the more common choice.

**1. Configure the broker:**

```properties
listeners=PLAINTEXT://:9092,SSL://:9093,SASL_SSL://:9094
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
  org.apache.kafka.common.security.scram.ScramLoginModule required;
```

**2. Provision the user (via ZooKeeper):**

```bash
kafka-configs.sh --zookeeper localhost:2181 --alter \
  --add-config 'SCRAM-SHA-512=[password=alice-secret]' \
  --entity-type users --entity-name alice
```

Credentials are stored as salted, hashed derivations — the original password cannot be recovered from ZooKeeper. To list or delete credentials, use `--describe` or `--delete-config`.

**3. Configure the client:**

```java
String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required\n" +
                        "username=\"alice\"\n" +
                        "password=\"alice-secret\";";
config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
config.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
```

**SASL/PLAIN** is similar but sends credentials in cleartext and does not use ZooKeeper for credential storage — credentials are defined directly in the JAAS configuration. Guided by Defence in Depth, SCRAM should be preferred over PLAIN. Note that SASL authentication is **incompatible with SSL client authentication** — when connecting over the `SASL_SSL` listener, `ssl.client.auth` settings are ignored.

#### Interbroker authentication

To upgrade interbroker communications to use authentication, create admin credentials and update `server.properties`:

```properties
inter.broker.listener.name=SASL_SSL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=\
  org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" password="admin-secret";
```

Harden the file permissions with `chmod 600 $KAFKA_HOME/config/server.properties`.

#### External JAAS configuration

Where in-line JAAS config is not supported, create a `kafka_server_jaas.conf` file:

```properties
sasl_ssl.KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="admin" password="admin-secret";
};
```

Then start the broker with:

```bash
KAFKA_OPTS=-Djava.security.auth.login.config=$KAFKA_HOME/config/kafka_server_jaas.conf
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

#### OAuth 2.0 bearer tokens

The OAUTHBEARER SASL mechanism enables the use of OAuth 2.0 Access Tokens to authenticate user principals. Unsecured JWS tokens (with `"alg":"none"`) function out-of-the-box with minimal configuration — the principal's username is taken from the `sub` claim in the JWT. For production, implement `AuthenticateCallbackHandler` on both the client (to generate signed tokens) and the broker (to validate them). The open-source Kafka OAuth project at github.com/jairsjunior/kafka-oauth provides a ready implementation.

#### Delegation tokens

Delegation tokens are a lightweight authentication mechanism complementary to SASL, introduced in KIP-48 (release 1.1.0). They simplify key distribution for ephemeral worker nodes in stream processing — the coordinator creates a time-bounded token and hands it to each worker, so long-lived credentials never leave the coordinator.

**Key concepts:**

- **Lifespan:** Hard upper bound on token age (`delegation.token.max.lifetime.ms`, default 7 days).
- **Expiry:** Soft limit for token use (`delegation.token.expiry.time.ms`, default 24 hours).
- **Renewal:** Extends the expiry time, subject to the lifespan limit.
- **Purging:** Expired tokens are asynchronously purged from ZooKeeper.

**Enable delegation tokens on the broker:**

```properties
delegation.token.master.key=secret-master-key
delegation.token.expiry.time.ms=3600000
delegation.token.max.lifetime.ms=7200000
```

The `delegation.token.master.key` is required and must be shared by all brokers.

**Creating tokens:**

```bash
kafka-delegation-tokens.sh --bootstrap-server localhost:9094 \
  --command-config client.properties --create --max-life-time-period -1 \
  --renewer-principal User:admin
```

**Client configuration:** The username is set to the token ID, password to the HMAC value, and `tokenauth="true"` must be present to distinguish token auth from username/password auth.

**Rotating secrets** is currently a three-step process: (1) expire all existing tokens, (2) roll the cluster with a new master key, (3) generate and distribute new tokens.

### ZooKeeper authentication

ZooKeeper supports SASL client authentication using DIGEST-MD5. ZooKeeper authentication powers its authorization model via ACLs on znodes (CREATE, READ, WRITE, DELETE, ADMIN permissions). ZooKeeper 3.5.6 does not enforce authentication — only authorization — meaning unauthenticated connections are accepted but restricted.

To enable ZooKeeper authentication, create `zookeeper_jaas.conf` and `kafka_server_jaas.conf` with the appropriate `DigestLoginModule` configuration, then run `zookeeper-security-migration.sh --zookeeper.acl secure` to migrate existing znodes. Configure the broker with `zookeeper.set.acl=true`. CLI tools also require the JAAS file via `KAFKA_OPTS`.

### Configuring CLI tools and Kafdrop

Once Kafka is secured, CLI tools must be configured with `client.properties` containing `security.protocol=SASL_SSL`, the truststore location, and SASL credentials. Supply the file via the `--command-config` flag.

For Kafdrop, copy the truststore as `kafka.truststore.jks` and create a `kafka.properties` file with the security configuration. Start Kafdrop connecting to the SASL_SSL port.

## Authorization

With SSL and authentication in place, authorization provides fine-grained control over what authenticated clients are allowed to do. Kafka implements authorization via resource-centric Access Control Lists (ACLs).

An ACL specifies:

- **Principal** — the user performing the operation
- **Operation** — the action (Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, All)
- **Host** — optional network address restriction
- **Resource type** — Cluster, DelegationToken, Group, Topic, or TransactionalId
- **Resource pattern** — literal or prefixed matching
- **Outcome** — Allow or Deny

Resource types and their supported operations:

- **Cluster:** Alter, AlterConfigs, ClusterAction, Create, Describe, DescribeConfigs
- **DelegationToken:** Describe
- **Group:** Delete, Describe, Read
- **Topic:** Alter, AlterConfigs, Create, Delete, Describe, DescribeConfigs, Read, Write
- **TransactionalId:** Describe, Write

### Enable authorization

```properties
authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer
super.users=User:admin
```

Kafka takes a **default-deny** (positive/additive) stance — unless an action is explicitly allowed, it is denied. This can be inverted with `allow.everyone.if.no.acl.found=true`, but the default-deny model is recommended as it starts from a blank slate and adds permissions on a needs basis.

### Managing ACLs

```bash
# Grant Read and Write on a topic
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --allow-principal User:alice \
  --operation Read --operation Write --topic getting-started

# Grant IdempotentWrite on the cluster
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --allow-principal User:alice \
  --operation IdempotentWrite --cluster

# Grant Read on a consumer group
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --allow-principal User:alice \
  --operation Read --group basic-consumer-sample
```

### World-readable topics and mixing allow/deny

To make a topic readable by all authenticated users without listing them individually, use the wildcard `User:"*"`:

```bash
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --allow-principal User:"*" --operation Read --topic guest-readable
```

To exclude a specific user from a topic (e.g., guests from `trusted-only`), combine allow and deny:

```bash
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --allow-principal User:"*" --operation Read --topic trusted-only

kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --deny-principal User:guest --operation Read --topic trusted-only
```

Deny rules take precedence over allow rules. Always use an allow rule over a broader-matching pattern than a deny rule. This gives up to three rule tiers: default-deny (outermost), custom allow, custom deny (innermost).

### Prefixed resource patterns

Introduced in KIP-290 (release 2.0.0), prefixed ACLs allow matching multiple resources sharing a common prefix:

```bash
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --add --deny-principal User:guest \
  --resource-pattern-type=prefixed \
  --operation Read --topic trusted
```

### Listing and bulk-removal

```bash
# List all ACLs
kafka-acls.sh --command-config client.properties --bootstrap-server localhost:9094 --list

# View all rules affecting a specific resource (including prefixed matches)
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --list --resource-pattern-type=match --topic trusted-only

# Bulk remove matching rules
kafka-acls.sh --bootstrap-server localhost:9094 --command-config client.properties \
  --remove --resource-pattern-type=match --topic trusted-only --force
```

### Network address restrictions

Kafka supports restricting ACLs to specific IP addresses using `--allow-host` or `--deny-host` flags. Multiple flags may be specified in a single command.

### Common authorization scenarios

| Scenario             | Required ACLs                                                                               |
| -------------------- | ------------------------------------------------------------------------------------------- |
| Create/delete topics | `ClusterAction` on Cluster                                                                  |
| Publish to a topic   | `Write` (+ `IdempotentWrite` on Cluster if idempotence enabled) and `Describe` on the topic |
| Consume from a topic | `Read` and `Describe` on the topic + `Read` on the consumer group                           |
| Use the admin client | `DescribeConfigs`/`AlterConfigs` on the appropriate resource types                          |

Always use separate credentials for different application entities and grant only the minimal privileges required. Avoid using admin users for routine operations. The recommended approach is to test ACLs against a staging cluster before applying them to production.

---

# Chapter 17: Quotas

There has been a strong emphasis throughout this book on Kafka’s role as a proverbial ‘glue’ that binds disparate systems. At the very core of the event streaming paradigm is the notion of multiple tenancies.

Chapter 16: Security has set the foundation for multitenancy — delineating how clients having different roles and objectives can securely connect to, and share broker infrastructure — the underlying topics, consumer groups and other resource types. Despite the unmistakable overlap between security and quotas, the latter stands on its own. The discussion on quotas transcends security, affecting areas such as quality of service and capacity planning. In saying that, the use of quotas requires authentication controls; therefore, Chapter 16: Security is a prerequisite for this chapter.

The examples in this chapter will not work unless authentication has been enabled on the broker and suitable client-side preparations have been made. If you have not yet read Chapter 16: Security and worked through the examples, please do so before proceeding with the material below.

## The rationale behind quotas

### Mitigating denial of service attacks

As it has been just said, the discussion on quotas is a logical extension of the ‘security’ topic. At the heart of information security are three primordial concepts, often referred to as the CIA triad. (Not to be confused with the intelligence agency.) The acronym deciphers to confidentiality, integrity and availability — phrased in relation to information assets. Chapter 16: Security touched on all aspects of the CIA triad, but focused mostly on confidentiality and integrity, using specific security controls such as encryption (TLS), X.509 certificates, authentication (Kerberos, SASL, OAuth) and authorization (ACLs). The availability aspect is partly catered to by the authorization control: by ensuring that parties are only acting in a manner that has been prescribed for them, we can protect other parties from unsanctioned interference.

The flexibility of Kafka’s rule-based ACLs enables us to apply varying levels of assurance to different resources. We might have low-assurance topics collocated with high-assurance topics, where a greater number of semi-trusted clients may be allowed to access the former, admitting a much more select group of clients to the latter. In another scenario, we might be operating a SaaS business, where resources are partitioned on a per-customer basis and where it is essential that customers cannot access or manipulate each other’s data.

Even with fine-grained ACLs in place, an authorized client with the lowest level of access may attempt to monopolise cluster resources with the intent of disrupting the operation of legitimate clients. This might be a client with read-only access to some innocuous topic. Alternatively, in the SaaS scenario, it may be a low-tier paying customer whose sole intention is to saturate the service provider and thereby cause financial harm.

The mechanism for a denial of service (DoS) attack is fairly straightforward. Once a client gains access to a resource, it can generate large volumes of read queries or write requests (depending on its level of access), thus causing network congestion, memory pressure and I/O load on the brokers. As these are all finite resources, their disproportionate consumption by one client creates starvation for the rest.

Quotas fill the gap left by ACLs, specifying the extent of resource utilisation that is to be accorded to a user. Where that limit has been breached, the brokers will automatically activate mitigating controls, throttling a client until its request profile complies with the set quota. There are no further penalties applied to the offending client beyond throttling. This is intended, as aside from a traffic spike, there is nothing to suggest that a client is malicious; it may simply be responding to elevated levels of demand.

### Capacity planning

With multiple clients contending for the use of a common Kafka cluster, how can the operator be sure that the finite resources available to the cluster are sufficient to meet the needs of its clients? This answer leads to the broader topic of capacity planning. This is a complex, multi-disciplined topic that includes elements of measurement, modelling, forecasting and optimisation. And it is fair to say that this material is well outside our scope. However, the first step of capacity planning is modelling the demand, and quotas are remarkably helpful in this regard. If all current and prospective users of a cluster have been identified, and each has had their quotas negotiated, then the aggregate ‘peak’ demand can be determined. Whether or not the cluster’s resources (disks, CPU, memory, network bandwidth, etc.) will be provisioned to cover the worst-case demand is a separate matter — the balance of cost and willingness to accept the risk of failing to meet demand.

### Quality of service

Quality of service (QoS) is strongly related to both the security and capacity aspects. QoS focuses on the customer (or the client, in the context of Kafka), ensuring that the latter receives a service that meets or exceeds the baseline warranted by the service provider. This measure is, in simple terms, a function of the provider’s ability to furnish sufficient capacity when it is called for, as well as its resistance to DoS attacks.

QoS naturally dovetails into operational-level agreements (OLAs) — arrangements between collaborating parties that influence the consuming party’s ability to provide a service to its downstream consumer — ultimately affecting support-level agreements (SLAs) at the organisation’s boundary. Without specific QoS guarantees, OLAs cannot be reliably fulfilled — that is, in the absence of excessive over-provisioning of resources. The latter is expensive and wasteful; in most cases, it is more economically viable to ration resources than to purchase excess capacity, unless the cost of rationing exceeds the cost of the resources that are being preserved.

When operating a small cluster with only a handful of connected clients, the baseline capacity is often already in excess of the peak demand, particularly when the cluster comprises multiple brokers for availability and durability. In such deployments, Kafka’s efficiency provides for ample headroom, and managing quotas may not be the most productive use of an operator’s time and resources. As the number of clients grows and their diversity broadens, the need to manage quotas becomes more apparent.

## Types of quotas

Kafka supports two types of quotas:

1. **Network bandwidth quotas** — inhibit producers and consumers from transferring data above a set rate, measured in bytes per second.
2. **Request rate quotas** — limit a client’s CPU utilisation on the broker as a percentage of one network or I/O thread.

Quotas (both types) are defined on a per-broker basis. In other words, a quota is enforced locally within an individual broker — irrespective of what the client may be doing on other brokers. If a client connects to two brokers, it will be served the equivalent of two quotas; a multiple of $N$ quotas for $N$ brokers. Even though a client may receive a multiple of its original quota, it can never exceed the original quota limit on any given broker.

### Network bandwidth quotas

Network bandwidth quotas rely on the amount of transferred data as a definitive metric for assessing a client’s utilisation of a broker’s available resources. It may be thought of as a compound metric — increasing the rate of data transfer places a greater strain on the network, but also commensurately utilises the I/O channels on the broker, and may lead to increased memory pressure (due to buffering). In addition, when the client connects to an SSL listener, the broker loses its ability to employ the zero-copy optimisation, involving the CPU to encrypt and decrypt network data. With SSL enabled, the greater the transfer rate, the greater the load on the CPU.

A network bandwidth quota is specified as a pair of values: a `producer_byte_rate` and a `consumer_byte_rate` — representing the upper bound on the allowable bandwidth, in bytes per second (B/s).

Quotas are enforced by sampling the client’s activity over a period of time, using a rudimentary sliding window algorithm. A pair of broker properties — `quota.window.num` and `quota.window.size.seconds` — stipulate the number of samples $N$ retained and the duration of each sampling period $S$, respectively. The default values are 1 (for the sample duration) and 11 (for the number of samples). Collectively, the samples represent a sliding window.

When the observed utilisation $U$ exceeds the quota $Q$, the broker will penalise the client by introducing an artificial delay $D$ into the response:

$$D = T \frac{U - Q}{Q}$$

Where $T$ is the product of `quota.window.num` and `quota.window.size.seconds`. The duty cycle $Y$ of the system is given by:

$$Y = \frac{T}{D + T}$$

### Request rate quotas

Request rate quotas were introduced in KIP-124 (release 0.11.0.0) to prevent denial of service from frequent protocol activity and address scenarios where clients utilise the broker’s CPU disproportionately to the request/response size (e.g., mismatched compression settings, TLS overhead, unauthorised requests).

Request quotas are configured as a fraction of overall time a client is allowed to occupy request handler (I/O) threads and network threads within each quota window. For example, a quota of 50% implies that half of a thread can be utilised on average within the measured time window. A quota of 200% is the equivalent of two full-time threads. The underlying mechanism for enforcing request rate quotas is virtually identical to the one used for network bandwidth quotas, piggybacking on the same sliding window properties. Unlike network quotas, request rate quotas are dilation-resistant for vertical scaling only — they are based on the absolute values of `num.io.threads` and `num.network.threads`.

**Worked example:** With default window settings ($T = 11$ s), consider three clients with a 20,000 B/s quota:

- Client C0 publishes at 14 kB/s — no penalty (under quota).
- Client C1 publishes at 36 kB/s — $D = 11 \times (36000 - 20000) / 20000 = 8.8$ s delay.
- Client C2 publishes at 100 kB/s — $D = 11 \times (100000 - 20000) / 20000 = 44$ s delay, yielding a duty cycle of only 20%.

## Subject affinity and precedence order

Quotas apply to two entity types: **user principals** and **client IDs**. Quotas are defined for either usernames or client IDs individually, or may cover a combination of the two. There are a total of eight ways a quota may be associated with its subject.

The precedence order (from highest to lowest) is:

1. Specific username + Specific client ID
2. Specific username + Default client ID
3. Specific username + Unspecified client ID
4. Default username + Specific client ID
5. Default username + Default client ID
6. Default username + Unspecified client ID
7. Unspecified username + Specific client ID
8. Unspecified username + Default client ID

Upon the consumption of a resource, Kafka will iterate through the rules in the order of increasing precedence number, stopping at the first rule that matches the client’s attributes.

## Applying quotas

Quotas are configured using the `kafka-configs.sh` CLI, targeting either (or both) `users` and `clients` entity types.

```bash
$KAFKA_HOME/bin/kafka-configs.sh \
  --zookeeper localhost:2181 --alter --add-config \
  'producer_byte_rate=100000' \
  --entity-type users --entity-name alice \
  --entity-type clients --entity-name pump
```

### Buffering and timeouts

When a producer is throttled, records buffer up in the client's memory. The `buffer.memory` property (default 32 MiB) sets an upper bound on this memory. If records are buffered faster than they can be delivered, the producer will eventually block. If the delay exceeds `delivery.timeout.ms`, the records will expire and throw a `TimeoutException`. To prevent this, you can either increase the delivery timeout or decrease the buffer memory to create backpressure on the application.

### Sensing quota enforcement

One of the well-known challenges of Kafka’s quota implementation is the lack of explicit communication of the quota’s enforcement from the broker to the client. The client simply observes degraded performance or timeouts. To mitigate this, applications can track in-flight records or limit the `buffer.memory` property to create natural backpressure.

### Tuning the duration and number of sampling windows

By increasing `quota.window.num`, the overall duration of the window $T$ is increased, elevating the tolerance for bursty traffic. Conversely, reducing the overall window duration leads to a flatter traffic profile. By increasing `quota.window.size.seconds`, the granularity of the sampling process is decreased, thereby inflating the magnitude of the "snapback" effect when the sliding window advances. The default value of 1 second for the sampling period is generally recommended to avoid pronounced discontinuities in the resulting throughput.

---

# Chapter 18: Transactions

When discussing event stream processing applications, one topic of conversation that invariably comes up is that of delivery guarantees.

This chapter looks at one last control made available by Kafka — transactions. Transactions fill certain gaps of idempotent consumers with respect to the side-effects of record processing, and enable end-to-end, **exactly-once** transactional semantics across a series of loosely-coupled stream processing stages.

## Preamble

Transactions arrived in release 0.11.0.0, as part of a much larger KIP-98 — an undertaking of approximately 60 work items with over three years of planning, a nine-month public review, and 15,000+ lines of unit tests. The significance of this may not be immediately apparent, but the reader has already witnessed this KIP in action in Chapter 10: Client Configuration — namely, the `enable.idempotence` property. Both the idempotent producer and transactional messaging features are highly related and share a great deal in common. The performance overhead of transactions is estimated at 3–5%.

## The rationale behind transactions

### The problem: Duplicate records

Under a conventional consume-transform-produce model, the consumer side of a stage will read a record from the input topic, apply a transformation, and publish a corresponding record on the output topic. Once it receives an acknowledgement, it will commit the offsets of the input record.

There are five failure cases to consider in this model:

1. **Failure before consumption:** No effect — the input record will be redelivered.
2. **Failure after consumption but before transformation:** The consumer recovers from the last committed offset and processes the record again.
3. **Failure after transformation but before publishing:** The output record is lost; recovery replays the input record.
4. **Failure _after_ publishing the output record but _before_ committing the input offsets** — the core problem. The recovering process will replay the input record, resulting in two output records for the same input record. This is the essence of **at-least-once** delivery.
5. **Failure after committing offsets:** No duplication, but the output record may have been processed.

Without a secondary index, Kafka cannot natively deduplicate records based on application-level IDs. What if Kafka was used as primary storage — the proverbial ‘source of truth’; for example, acting in the role of an event store in an event sourcing system? It would hardly be acceptable to have two records representing the same logical event.

### The solution: Transactions

The transactional messaging capability strengthens Kafka’s delivery semantics by introducing limited **Atomicity**, **Consistency**, and **Isolation** guarantees on top of the existing **Durability** pledge (ACID).

- **Atomicity:** Ensures that for a group of records published within an encompassing transaction scope, either all records are visibly persisted to their respective logs, or none are persisted.
- **Consistency:** Ensures that the cluster transitions from one valid state to another. An input record must result in an output record once consumed, or it must not be consumed at all.
- **Isolation:** Ensures that the effects of a transaction cannot be externally visible until it commits.

## Transactions under the hood

### Role of the transaction coordinator

At the heart of the implementation is a unique **Producer ID (PID)** that is assigned by a **transaction coordinator** — a module within a broker — for the duration of the producer’s session. The coordinator is load-balanced via the internal `__transaction_state` topic in a manner similar to consumer groups. Transactional messaging builds upon this infrastructure by increasing the lifetime of a producer’s PID such that it survives a single producer session. This is achieved by specifying an optional `transactional.id` property on the producer (default expiration: one week, controlled by `transactional.id.expiration.ms`).

The epoch acts as a fencing mechanism, blocking **zombie** processes that have been displaced by a newer PID assignment. A `ProducerFencedException` is thrown when the producer attempts to manipulate a transaction that has been fenced off.

### Producer API enhancements

Transactional messaging adds several methods to the Producer API, operating through a state machine with states: `READY`, `IN_TRANSACTION`, `COMMITTING_TRANSACTION`, `ABORTING_TRANSACTION`:

- `initTransactions()`: Initialises the transactional subsystem and fences zombies (assigns a PID with epoch).
- `beginTransaction()`: Demarcates the start of a transaction scope (transitions to `IN_TRANSACTION`).
- `sendOffsetsToTransaction()`: Incorporates consumer-side offsets into the scope of the current transaction.
- `commitTransaction()`: Flushes unsent records and commits the transaction (transitions through `COMMITTING_TRANSACTION` back to `READY`).
- `abortTransaction()`: Discards pending records and aborts the transaction (transitions through `ABORTING_TRANSACTION` back to `READY`).

On the consumer side, consumers must piggyback on the producer to commit offsets atomically — `commitSync()` / `commitAsync()` cannot be used, as they operate outside the transaction scope.

### Assigning a transactional ID

The most perplexing aspect of transaction management is the choice of the transactional ID. It must survive producer sessions and act as a fencing mechanism. Several alternatives exist:

- **Shared transactional ID:** If all consumers use the same ID, a fencing collision prevents any from making progress.
- **Random UUID:** No fencing capability — a zombie process cannot be distinguished.
- **The recommended approach** is to replace the singleton producer with a collection of producers — **one for each assigned partition in the input set**. The transactional ID for each producer instance is derived by concatenating the corresponding input topic and partition index pair (e.g., `tx-input-2`).

By pinning a producer client instance to the input topic-partition, we capture the causality among input and output records within the identity of the producer. As the partition assignment changes on the group coordinator, the causal relationship is carried forward to the new assignee; the outgoing assignee will fail if it attempts to publish a record under the same identity. The producers may be lazily initialised within a `ConsumerRebalanceListener`.

### Transactional consumers

To enable transactional semantics on a consumer, the `isolation.level` must be set to `read_committed`. Within this mode, the consumer replaces its notion of end offsets with the **Last Stable Offset (LSO)** — the minimum of the high-water mark and the smallest offset of any open transaction. Under the constraint of the LSO, a consumer will not be allowed to enter a region in the log that contains an open transaction until that transaction commits or aborts.

When a transaction aborts, Kafka does not delete records from the affected partitions — being an append-only ledger. Instead, an **abort marker** is written, and `read_committed` consumers skip over the aborted records transparently.

## Simple stream processing example

The following example demonstrates the consume-transform-produce loop with pinned producers:

```java
// Pinned Producers mapping
String transactionalId = topicPartition.topic() + "-" + topicPartition.partition();
config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
Producer<String, Integer> producer = new KafkaProducer<>(config);
producer.initTransactions();

// Consume-Transform-Produce Loop
producer.beginTransaction();
try {
    producer.send(outRec);
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(topicPartition, new OffsetAndMetadata(inRec.offset() + 1));
    producer.sendOffsetsToTransaction(offsets, groupId);
    producer.commitTransaction();
} catch (KafkaException e) {
    producer.abortTransaction();
    throw e;
}
```

The complete source code for a working example — including `PinnedProducers`, `TransformStage`, `InputStage`, and `OutputStage` — is available at github.com/ekoutanov/effectivekafka.

## Limitations

1. **Bound to Kafka resources:** Kafka does not support standard transaction APIs such as XA or JTA.
2. **Cannot span producers:** Transactions cannot be used to span multiple producer instances with different transactional IDs.
3. **Cannot span clusters:** Consumer-side offsets cannot be committed via a transaction coordinator residing in a different cluster.
4. **May be partially observed:** A consumer using `read_committed` may still partially observe a transaction across multiple partitions where some partitions have committed and others have not. Log segment deletion and compaction may also remove uncommitted transaction markers.
5. **Incomplete exactly-once semantics:** Transactions do nothing to prevent an input record from being handled twice if the processing stage has non-idempotent side effects (e.g., writing to an external database). The application must still ensure idempotence for external resources.

## Are transactions over-hyped?

For the majority of event-driven applications, the most useful and practical aspect of transactional messaging is the **idempotence guarantee on the producer** (`enable.idempotence=true`). This feature utilises the same underlying PID concept, ensuring that records do not arrive out-of-order or in duplicate on the broker within the delivery timeout, without the complexity of managing pinned transactional producers.

**Kafka Streams** can transparently deal with transactions — it handles the mapping of pinned producers and the consumption of transactional data internally. For applications built on Kafka Streams, exactly-once semantics are available with minimal additional complexity.

It should be acknowledged that the exactly-once impossibility dictum does not take anything away from Kafka; the release of transactional messaging is nonetheless useful in a limited sense. There is no silver bullet — exactly-once semantics are not possible at the middleware layer without tight-knit collaboration with the application. Where the application domain does not fit entirely into the limiting case for which transactional messaging holds, the reader ought to take their own measures in ensuring idempotence across all affected resources.

---

# Appendix A: Modern Kafka Migration

> This appendix bridges the gap between the book's original coverage (Kafka 2.4, ZooKeeper-based) and modern Apache Kafka 4.3.x (KRaft-only, with new protocols and features). Consult the `docs/` folder in this repository for the complete official documentation.

---

## A.1 KRaft: ZooKeeper Is Gone

The book's architecture chapter (Ch3) describes ZooKeeper as a core component for controller election and metadata storage. **As of Apache Kafka 4.0+, ZooKeeper mode has been completely removed.**

**What changed:**

| Aspect              | Book Era (Kafka 2.4)               | Modern Kafka (4.3.x)                                                                |
| ------------------- | ---------------------------------- | ----------------------------------------------------------------------------------- |
| Controller election | ZooKeeper ephemeral node           | Raft quorum among controller nodes                                                  |
| Metadata storage    | ZooKeeper                          | Internal `__cluster_metadata` log replicated via Raft                               |
| Cluster membership  | ZooKeeper `/brokers/ids` path      | Controller quorum with heartbeat-based session tracking                             |
| Required servers    | Kafka brokers + ZooKeeper ensemble | Only Kafka servers (controllers + brokers)                                          |
| `server.properties` | `zookeeper.connect=` required      | No ZooKeeper config; uses `process.roles` and `controller.quorum.bootstrap.servers` |

**Key configuration changes:**

```properties
# Book era (Kafka 2.4) — ZooKeeper
zookeeper.connect=localhost:2181
broker.id=0

# Modern Kafka (4.3+) — KRaft
process.roles=broker,controller          # or just 'broker' / 'controller'
node.id=0
controller.quorum.bootstrap.servers=localhost:9093
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
```

**Startup sequence:**

```bash
# Book era
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties

# Modern Kafka (4.3+)
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties
kafka-server-start.sh config/server.properties
```

**Controller provisioning (Kafka 4.3+):**

- **Standalone:** `kafka-storage.sh format --standalone -t $CLUSTER_ID -c config/controller.properties`
- **Multiple controllers:** Use `--initial-controllers "0@host:port:uuid,..."` flag
- **Adding controllers dynamically** (Kafka 4.1+): Use `kafka-metadata-quorum.sh add-controller`
- **Recommended:** 3 or 5 dedicated controllers for production; combined mode (`process.roles=broker,controller`) for dev/test

---

## A.2 Consumer Rebalance Protocol: KIP-848

The book's Chapter 15 describes consumer rebalancing with eager and cooperative (incremental) strategies using client-side assignors. **Since Apache Kafka 4.0, a new Consumer rebalance protocol (KIP-848) is GA.**

**What changed:**

| Aspect            | Classic Protocol                              | Consumer Protocol (KIP-848)                             |
| ----------------- | --------------------------------------------- | ------------------------------------------------------- |
| Assignor location | Client-side (`partition.assignment.strategy`) | Server-side (`group.consumer.assignors`)                |
| Heartbeat control | Client configs (`heartbeat.interval.ms`)      | Server configs (`group.consumer.heartbeat.interval.ms`) |
| Session timeout   | Client config (`session.timeout.ms`)          | Server config (`group.consumer.session.timeout.ms`)     |
| Rebalance design  | Global sync barrier                           | Fully incremental, no barrier                           |
| Default assignor  | `RangeAssignor` (client)                      | `uniform` (server)                                      |

**To enable the new protocol:**

```properties
# Consumer client config
group.protocol=consumer
```

**When the new protocol is enabled, these configs are no longer usable:**

- `heartbeat.interval.ms`
- `session.timeout.ms`
- `partition.assignment.strategy`
- `enforceRebalance()` / `enforceRebalance(String)`

**Migration path:** Consumer groups automatically convert from Classic to Consumer and vice versa when empty. For online migration, roll out consumers with `group.protocol=consumer` — the first new-protocol consumer joining triggers conversion. Downgrade reverses when the last new-protocol consumer leaves.

**Evolution timeline (KIP-1274):**

| Version   | Status                                                 |
| --------- | ------------------------------------------------------ |
| Kafka 3.7 | Early Access                                           |
| Kafka 4.0 | GA (production-ready)                                  |
| Kafka 5.0 | Defaults to Consumer protocol; Classic still supported |
| Kafka 6.0 | Only Consumer protocol supported                       |

---

## A.3 Share Groups (KIP-932)

The book does not cover share groups, as they were introduced after publication. **Since Apache Kafka 4.2, share groups are GA.**

Share groups are an alternative to consumer groups where multiple consumers can cooperatively consume records from the same partition. Key differences from consumer groups:

| Aspect                 | Consumer Group                | Share Group                                                            |
| ---------------------- | ----------------------------- | ---------------------------------------------------------------------- |
| Partition assignment   | Each partition → one consumer | Multiple consumers can read same partition                             |
| Max consumers          | Cannot exceed partition count | Can exceed partition count                                             |
| Record acknowledgement | Offset-based (batch commit)   | Per-record acknowledgement                                             |
| Delivery tracking      | Not tracked                   | Delivery attempts counted; automatic handling of unprocessable records |
| Ordering               | Total order per partition     | No ordering guarantee                                                  |

**When to use share groups:** Queue-like workloads where records are processed one at a time, rather than as part of an ordered stream.

**Configuration:**

```properties
# Share consumer
group.id=my-share-group
# No group.protocol needed — share groups use their own protocol
```

**Server-side configs (Kafka 4.3):**

- `share.delivery.count.limit` — max delivery attempts
- `share.partition.max.record.locks` — max acquired records per partition
- `share.record.lock.duration.ms` — acquisition lock duration (default: 30s)
- `share.renew.acknowledge.enable` — enable renewal acknowledgements

---

## A.4 Streams Rebalance Protocol (KIP-1071)

The book's Kafka Streams coverage assumes the classic consumer group rebalance protocol. **Since Kafka 4.2, the Streams Rebalance Protocol is production-ready** for its core feature set. This broker-driven rebalancing system provides faster, more stable rebalances and better observability for Kafka Streams applications.

---

## A.5 Kafka Streams Scala Library Deprecation

**As of Kafka 4.3**, the `kafka-streams-scala` library is deprecated and will be removed in Kafka 5.0. Migrate to the Java Kafka Streams API or use the Scala migration guide in the official documentation.

---

## A.6 Tiered Storage

The book briefly mentions tiered storage as a "newer feature." **As of Kafka 4.x, tiered storage is GA** and has several new configuration options:

| Config (Kafka 4.3)                         | Purpose                                                            |
| ------------------------------------------ | ------------------------------------------------------------------ |
| `remote.log.metadata.topic.min.isr`        | Min ISR for internal `__remote_log_metadata` topic (default: 2)    |
| `follower.fetch.last.tiered.offset.enable` | New followers bootstrap from last tiered offset (default: `false`) |
| `remote.log.metadata.admin.<property>`     | Prefix for admin client config used by `RemoteLogMetadataManager`  |

> **Deprecated:** `remote.log.manager.thread.pool.size` — use `remote.log.manager.follower.thread.pool.size` instead.

**Tiered storage considerations:**

- Requires a single mount point (does not support JBOD)
- When using end-to-end encryption, compression should be disabled (entropy already maximal)
- The `EARLIEST_PENDING_UPLOAD_TIMESTAMP` (-6) timestamp type was added to `ListOffsets` API (version 11)

---

## A.7 Java Version Requirements

The book assumes Java 8/11. **Modern Kafka 4.x requirements:**

| Module                | Minimum Java | Supported Versions   |
| --------------------- | ------------ | -------------------- |
| Brokers & Controllers | 17           | 17, 21, 25           |
| Clients & Streams     | 11           | 11, 17, 21, 25       |
| Java 8                | —            | Removed in Kafka 4.0 |

---

## A.8 Dynamic KRaft Controllers (KIP-853)

**Kafka 4.1+** supports dynamic controller quorums, allowing controllers to be added or removed at runtime without restarting the cluster:

```bash
# Add a controller
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 add-controller \
  --command-config controller.properties

# Remove a controller
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 remove-controller \
  --controller-id <id> --controller-directory-id <directory-id>
```

Static quorum (using `controller.quorum.voters`) is still supported but deprecated in favour of dynamic quorum (using `controller.quorum.bootstrap.servers`). Check your quorum type with `kafka-features.sh describe` — if `kraft.version` is level 0, you're using a static quorum; level 1+ means dynamic.

---

## A.9 Eligible Leader Replicas (ELR)

**Kafka 4.x** introduces Eligible Leader Replicas (ELR), which allow a broader set of replicas to be eligible for leader election, improving availability during extended outages. When ELR is enabled, the semantics of `min.insync.replicas` change — ensure you review the [ELR documentation](https://kafka.apache.org/43/documentation.html#eligible_leader_replicas) before enabling in production.

---

## A.10 Broker Cordoning (KIP-1066)

**Kafka 4.3** introduces the `cordoned.log.dirs` broker config for safe broker decommissioning. Mark directories as off-limits for new partition placement, let partitions migrate away via Cruise Control or manual reassignment, then remove the broker.

---

## A.11 CLI Tool Changes

The book's CLI examples use several tools with changed syntax or behaviour in modern Kafka:

| Tool                           | Book Era                                        | Modern Kafka                                            |
| ------------------------------ | ----------------------------------------------- | ------------------------------------------------------- |
| `kafka-topics.sh`              | `--zookeeper` for most operations               | `--bootstrap-server` (ZooKeeper not available)          |
| `kafka-configs.sh`             | `--zookeeper` for topic configs                 | `--bootstrap-server` for most operations                |
| `kafka-reassign-partitions.sh` | `--zookeeper` required                          | `--bootstrap-server` preferred                          |
| `kafka-broker-api-versions.sh` | Standalone tool                                 | Deprecated; use `kafka-cluster.sh api-versions`         |
| `kafka-server-start.sh`        | Starts with ZK connection                       | Starts in KRaft mode (no ZK)                            |
| `kafka-acls.sh`                | `--authorizer-properties zookeeper.connect=...` | `--bootstrap-server --command-config client.properties` |

---

## A.12 Config Deprecations and Removals

| Deprecated/Removed Config               | Replacement / Action                                     |
| --------------------------------------- | -------------------------------------------------------- |
| `zookeeper.connect`                     | No replacement — KRaft mode only                         |
| `broker.id`                             | Use `node.id`                                            |
| `inter.broker.protocol.version`         | Managed via `metadata.version` / `kafka-features.sh`     |
| `reserved.broker.max.id`                | Not needed in KRaft                                      |
| `broker.id.generation.enable`           | Not needed in KRaft                                      |
| `controlled.shutdown.*`                 | Not needed in KRaft                                      |
| `password.encoder.*`                    | Not needed in KRaft                                      |
| `group.coordinator.rebalance.protocols` | Managed via feature versions (`kafka-features.sh`)       |
| `remote.log.manager.thread.pool.size`   | Use `remote.log.manager.follower.thread.pool.size`       |
| `log.cleaner.enable`                    | Deprecated — do not set to `false`                       |
| `kafka-streams-scala` library           | Migrate to Java Kafka Streams API (removed in Kafka 5.0) |

---

## A.13 Summary: Book-to-Modern Mapping

| Book Chapter   | Topic                                 | Modern Change                                                                                  |
| -------------- | ------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Ch3 (§3.1–3.3) | Cluster membership, Controller, KRaft | ZooKeeper removed; KRaft only; `process.roles`, dynamic quorums                                |
| Ch3 (§3.4)     | Replication                           | ELR feature added (Kafka 4.x)                                                                  |
| Ch4            | Installation                          | No ZooKeeper; `kafka-storage.sh format`; Java 17+ required                                     |
| Ch8            | Bootstrapping & Listeners             | Listener config unchanged; KRaft uses `controller.listener.names`                              |
| Ch9            | Broker Configuration                  | `node.id` replaces `broker.id`; no `zookeeper.connect`                                         |
| Ch10           | Client Configuration                  | New `group.protocol=consumer` for KIP-848; `isolation.level` for transactions                  |
| Ch15           | Group Membership                      | KIP-848 Consumer protocol (GA 4.0); Share groups (GA 4.2); Streams rebalance protocol (GA 4.2) |
| Ch18           | Transactions                          | Mostly unchanged; KIP-848 compatible                                                           |

---

_Refer to the official Kafka documentation in the `docs/` folder and at kafka.apache.org/documentation for complete, up-to-date details on all topics covered in this appendix._
