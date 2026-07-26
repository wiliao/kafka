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

For all its outstanding benefits, EDA is not a panacea and cannot supplant integrated or monolithic systems in all cases. For instances, EDA is not well-suited to synchronous interactions, as mutual or unilateral awareness among collaborating parties runs contrary to the grain of EDA and negates most of its benefits.

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

Below is the final part of the combined, cleaned-up Markdown file, covering **Chapters 17 and 18**. You can append this directly to the end of the previous output to complete the entire `Effective_Kafka.md` document.

All PDF extraction artifacts (such as broken words and erratic spacing) have been meticulously corrected for a professional reading experience.

---

````markdown
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

Request rate quotas were introduced to prevent denial of service from frequent protocol activity and address scenarios where clients utilise the broker’s CPU disproportionately to the request/response size (e.g., mismatched compression settings, TLS overhead, unauthorised requests).

Request quotas are configured as a fraction of overall time a client is allowed to occupy request handler (I/O) threads and network threads within each quota window. For example, a quota of 50% implies that half of a thread can be utilised on average within the measured time window. The underlying mechanism for enforcing request rate quotas is virtually identical to the one used for network bandwidth quotas, piggybacking on the same sliding window properties.

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
````

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

Transactions arrived in release 0.11.0.0, as part of a much larger KIP-98. The significance of this may not be immediately apparent, but the reader has already witnessed this KIP in action in Chapter 10: Client Configuration — namely, the `enable.idempotence` property. Both the idempotent producer and transactional messaging features are highly related and share a great deal in common.

## The rationale behind transactions

### The problem: Duplicate records

Under a conventional consume-transform-produce model, the consumer side of a stage will read a record from the input topic, apply a transformation, and publish a corresponding record on the output topic. Once it receives an acknowledgement, it will commit the offsets of the input record.

If the process fails _after_ publishing the output record but _before_ committing the input offsets, the recovering process will replay the input record, resulting in two output records for the same input record. This is the essence of **at-least-once** delivery.

Without a secondary index, Kafka cannot natively deduplicate records based on application-level IDs. What if Kafka was used as primary storage — the proverbial ‘source of truth’; for example, acting in the role of an event store in an event sourcing system? It would hardly be acceptable to have two records representing the same logical event.

### The solution: Transactions

The transactional messaging capability strengthens Kafka’s delivery semantics by introducing limited **Atomicity**, **Consistency**, and **Isolation** guarantees on top of the existing **Durability** pledge (ACID).

- **Atomicity:** Ensures that for a group of records published within an encompassing transaction scope, either all records are visibly persisted to their respective logs, or none are persisted.
- **Consistency:** Ensures that the cluster transitions from one valid state to another. An input record must result in an output record once consumed, or it must not be consumed at all.
- **Isolation:** Ensures that the effects of a transaction cannot be externally visible until it commits.

## Transactions under the hood

### Role of the transaction coordinator

At the heart of the implementation is a unique **Producer ID (PID)** that is assigned by a **transaction coordinator** for the duration of the producer’s session. Transactional messaging builds upon this infrastructure by increasing the lifetime of a producer’s PID such that it survives a single producer session. This is achieved by specifying an optional `transactional.id` property on the producer.

The epoch acts as a fencing mechanism, blocking **zombie** processes that have been displaced by a newer PID assignment. A `ProducerFencedException` is thrown when the producer attempts to manipulate a transaction that has been fenced off.

### Producer API enhancements

Transactional messaging adds several methods to the Producer API:

- `initTransactions()`: Initialises the transactional subsystem and fences zombies.
- `beginTransaction()`: Demarcates the start of a transaction scope.
- `sendOffsetsToTransaction()`: Incorporates consumer-side offsets into the scope of the current transaction.
- `commitTransaction()`: Flushes unsent records and commits the transaction.
- `abortTransaction()`: Discards pending records and aborts the transaction.

### Assigning a transactional ID

The most perplexing aspect of transaction management is the choice of the transactional ID. It must survive producer sessions and act as a fencing mechanism.

The recommended approach is to replace the singleton producer with a collection of producers — **one for each assigned partition in the input set**. The transactional ID for each producer instance is derived by concatenating the corresponding input topic and partition index pair (e.g., `tx-input-2`).

By pinning a producer client instance to the input topic-partition, we capture the causality among input and output records within the identity of the producer. As the partition assignment changes on the group coordinator, the causal relationship is carried forward to the new assignee; the outgoing assignee will fail if it attempts to publish a record under the same identity.

### Transactional consumers

To enable transactional semantics on a consumer, the `isolation.level` must be set to `read_committed`. Within this mode, the consumer replaces its notion of end offsets with the **Last Stable Offset (LSO)** — the minimum of the high-water mark and the smallest offset of any open transaction. Under the constraint of the LSO, a consumer will not be allowed to enter a region in the log that contains an open transaction until that transaction commits or aborts.

## Simple stream processing example

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

## Limitations

1. **Bound to Kafka resources:** Kafka does not support standard transaction APIs such as XA or JTA.
2. **Cannot span producers:** Transactions cannot be used to span multiple producer instances with different transactional IDs.
3. **Cannot span clusters:** Consumer-side offsets cannot be committed via a transaction coordinator residing in a different cluster.
4. **Incomplete exactly-once semantics:** Transactions do nothing to prevent an input record from being handled twice if the processing stage has non-idempotent side effects (e.g., writing to an external database). The application must still ensure idempotence for external resources.

## Are transactions over-hyped?

For the majority of event-driven applications, the most useful and practical aspect of transactional messaging is the **idempotence guarantee on the producer** (`enable.idempotence=true`). This feature utilises the same underlying PID concept, ensuring that records do not arrive out-of-order or in duplicate on the broker within the delivery timeout, without the complexity of managing pinned transactional producers.

It should be acknowledged that the exactly-once impossibility dictum does not take anything away from Kafka; the release of transactional messaging is nonetheless useful in a limited sense. Where the application domain does not fit entirely into the limiting case for which transactional messaging holds, the reader ought to take their own measures in ensuring idempotence across all affected resources.
