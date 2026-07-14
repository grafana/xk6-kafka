## ADDED Requirements

### Requirement: Emitted metrics use community names and correct k6 types

The Writer and Reader SHALL emit custom k6 metrics registered on the k6 metrics
registry, using the community `mostafa/xk6-kafka` v1 metric names. `*_count`
metrics SHALL be k6 **Counters**; `*_seconds`, `*_bytes` (per batch/fetch),
`*_size`, `*_offset`, and `*_lag` metrics SHALL be k6 **Trends**.

Because k6 **sums** counter samples across the test, counter metrics sourced
from franz-go hooks (which accumulate monotonic totals) SHALL be emitted as
per-flush **deltas** (the increase since the previous flush), so the summed
value reconstructs the true total. Emitting a running total each flush is
incorrect and MUST NOT be done. Trend metrics SHALL emit one sample per observed
value (buffered by the hooks, drained at flush), not an aggregate.

Writer metrics: `kafka_writer_write_count`, `kafka_writer_message_count`,
`kafka_writer_message_bytes`, `kafka_writer_error_count`,
`kafka_writer_retries_count`, `kafka_writer_dial_count` (counters);
`kafka_writer_write_seconds`, `kafka_writer_batch_seconds`,
`kafka_writer_wait_seconds`, `kafka_writer_dial_seconds`,
`kafka_writer_batch_size`, `kafka_writer_batch_bytes` (trends).

Reader metrics: `kafka_reader_message_count`, `kafka_reader_message_bytes`,
`kafka_reader_fetches_count`, `kafka_reader_error_count`,
`kafka_reader_rebalance_count`, `kafka_reader_timeouts_count`,
`kafka_reader_dial_count` (counters); `kafka_reader_fetch_seconds`,
`kafka_reader_read_seconds`, `kafka_reader_wait_seconds`,
`kafka_reader_dial_seconds`, `kafka_reader_fetch_bytes`,
`kafka_reader_fetch_size`, `kafka_reader_offset`, `kafka_reader_lag` (trends).

#### Scenario: Hook-sourced counters emit deltas, not totals

- **WHEN** a metric backed by a franz-go hook (e.g. `kafka_writer_dial_count`,
  `kafka_reader_fetches_count`) is flushed on two successive produce/consume
  calls
- **THEN** each flush emits only the increase since the previous flush, so the
  summed counter equals the true total and is not inflated

### Requirement: Metrics are tagged by topic where a topic is meaningful

Topic-scoped metrics SHALL carry a `topic` tag identifying the topic, and a
single `produce` or `consume` call that spans multiple topics SHALL attribute
them per topic (one sample set per distinct topic), never to a single topic.
Topic-scoped metrics are the message-level metrics (`*_message_count`,
`*_message_bytes`, `kafka_reader_lag`, `kafka_reader_offset`) and the
batch/fetch-level metrics for which franz-go supplies the topic (`*_batch_*`,
`*_fetch_*`, `*_write_seconds`, `*_read_seconds`).

Metrics that are not topic-scoped SHALL be emitted without a `topic` tag rather
than attributed to an arbitrary topic: connection-level (`*_dial_count`,
`*_dial_seconds`) and group-level (`kafka_reader_rebalance_count`).

#### Scenario: Multi-topic produce attributes counts per topic

- **WHEN** one `writer.produce` call sends messages to topics `a` and `b`
- **THEN** `kafka_writer_message_count` and `kafka_writer_message_bytes` are
  emitted per topic, each sample tagged with its own topic, summing to the
  batch totals

#### Scenario: Multi-topic group consume attributes counts per topic

- **WHEN** one `reader.consume` call on a group subscribed to `groupTopics`
  returns messages from topics `a` and `b`
- **THEN** `kafka_reader_message_count`, `kafka_reader_message_bytes`,
  `kafka_reader_lag`, and `kafka_reader_offset` are emitted per topic, each
  tagged with the topic the message came from

### Requirement: Writer emits produce metrics

A `Writer` SHALL emit its metrics to the VU sample buffer at the end of each
`produce` call, on the VU goroutine.

#### Scenario: Successful produce records message counts and bytes

- **WHEN** `writer.produce` successfully writes N messages
- **THEN** `kafka_writer_message_count` increases by N and
  `kafka_writer_message_bytes` by the total serialized key+value bytes,
  attributed per topic

#### Scenario: Produce failure records an error

- **WHEN** a `writer.produce` call fails
- **THEN** `kafka_writer_error_count` increases

#### Scenario: Metrics require the VU context

- **WHEN** metrics would be emitted with no VU state (e.g. init context)
- **THEN** no sample is pushed and no panic occurs

### Requirement: Reader emits consume metrics

A `Reader` SHALL emit its metrics to the VU sample buffer at the end of each
`consume` call, on the VU goroutine.

#### Scenario: Successful consume records message counts and bytes

- **WHEN** `reader.consume` returns N messages
- **THEN** `kafka_reader_message_count` increases by N and
  `kafka_reader_message_bytes` by the total key+value bytes, attributed per
  topic

#### Scenario: Lag is derived per message

- **WHEN** a message is consumed at offset O from a partition with high
  watermark H
- **THEN** `kafka_reader_lag` records `max(0, H - O - 1)` and
  `kafka_reader_offset` records O, tagged with the message's topic

#### Scenario: Consume timeout records a timeout

- **WHEN** a `reader.consume` call times out before reaching its limit
- **THEN** `kafka_reader_timeouts_count` increases

### Requirement: Omitted community metrics are documented, not emitted

The extension MUST document, rather than emit, community metrics that derive
from the `segmentio/kafka-go` stats structs and have no `twmb/franz-go`
equivalent. Such metrics SHALL be absent from the summary (not emitted as zero
or faked), and their absence SHALL be recorded in this change's own
user-facing docs (a README metrics section), independent of any other change.

#### Scenario: Queue gauges are absent

- **WHEN** a test inspects the summary for `kafka_reader_queue_length` or
  `kafka_reader_queue_capacity`
- **THEN** those metrics are absent (documented as unsupported), and their
  absence does not fail the run
