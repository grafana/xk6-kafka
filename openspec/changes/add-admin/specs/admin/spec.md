## ADDED Requirements

### Requirement: Creating a topic

`connection.createTopic(topicConfig)` SHALL create a Kafka topic via the
connection's client, using `topic`, `numPartitions`, `replicationFactor`,
`replicaAssignments`, and `configEntries`. `numPartitions` and
`replicationFactor` SHALL default to `1` when unset. When `replicaAssignments`
is provided, the assignment list SHALL fully determine the topic's layout — the
number of entries is the partition count and each entry's `replicas` is that
partition's placement — so both `numPartitions` and `replicationFactor` are
ignored. It SHALL validate input locally before
contacting the broker — rejecting an empty `topic` and any `replicaAssignments`
entry with a negative or duplicate `partition` — and throw on a bad value. It
SHALL run in the VU context, throw from the init context or after `close`, and
throw when the broker reports a per-topic error (e.g. the topic already exists).

#### Scenario: Create a topic with defaults

- **WHEN** `createTopic({ topic: "t" })` is called on a connected Connection in the VU context
- **THEN** the topic is created with one partition and replication factor one

#### Scenario: Create fails in init context

- **WHEN** `createTopic` is called from the init context (no VU state)
- **THEN** it throws rather than creating a topic

#### Scenario: Create surfaces broker errors

- **WHEN** `createTopic` is called for a topic that already exists
- **THEN** it throws an error reporting the topic and the broker's reason

#### Scenario: Create rejects an empty topic name

- **WHEN** `createTopic({ topic: "" })` is called
- **THEN** it throws locally without contacting the broker

#### Scenario: Create rejects invalid replica assignments

- **WHEN** `createTopic` is called with a `replicaAssignments` entry whose `partition` is negative or duplicates another entry's
- **THEN** it throws locally without contacting the broker

### Requirement: Deleting a topic

`connection.deleteTopic(topic)` SHALL delete the named topic via the connection's
client. It SHALL reject an empty `topic` locally, run in the VU context, throw
from the init context or after `close`, and throw when the broker reports a
per-topic error.

#### Scenario: Delete a topic

- **WHEN** `deleteTopic("t")` is called on a connected Connection in the VU context for an existing topic
- **THEN** the topic is removed from the cluster

#### Scenario: Delete fails in init context

- **WHEN** `deleteTopic` is called from the init context (no VU state)
- **THEN** it throws rather than deleting a topic

#### Scenario: Delete rejects an empty topic name

- **WHEN** `deleteTopic("")` is called
- **THEN** it throws locally without contacting the broker

### Requirement: Listing topics

`connection.listTopics()` SHALL return the names of all non-internal topics on
the cluster as a string array. It SHALL run in the VU context and throw from the
init context or after `close`.

#### Scenario: List topic names

- **WHEN** `listTopics()` is called on a connected Connection in the VU context
- **THEN** it returns the names of the cluster's non-internal topics

#### Scenario: List excludes internal topics

- **WHEN** `listTopics()` is called on a cluster that has internal topics (e.g. `__consumer_offsets`)
- **THEN** the returned array does not include the internal topic names
