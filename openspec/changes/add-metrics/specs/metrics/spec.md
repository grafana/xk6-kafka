## ADDED Requirements

### Requirement: Writer emits produce metrics

A `Writer` SHALL emit custom k6 metrics for its produce activity, registered on
the k6 metrics registry and pushed to the VU sample buffer, using the community
`mostafa/xk6-kafka` v1 metric names. Counters SHALL accumulate over the test;
trends SHALL record per-observation values. Samples SHALL carry a `topic` tag
when a topic is known.

The Writer metrics are: `kafka_writer_write_count`,
`kafka_writer_message_count`, `kafka_writer_message_bytes` (counters);
`kafka_writer_error_count`, `kafka_writer_retries_count`,
`kafka_writer_dial_count` (counters); `kafka_writer_write_seconds`,
`kafka_writer_batch_seconds`, `kafka_writer_wait_seconds`,
`kafka_writer_dial_seconds`, `kafka_writer_batch_size`,
`kafka_writer_batch_bytes` (trends).

#### Scenario: Successful produce records message counts and bytes

- **WHEN** `writer.produce` successfully writes N messages to a topic
- **THEN** `kafka_writer_message_count` increases by N, `kafka_writer_write_count`
  increases, and `kafka_writer_message_bytes` increases by the total serialized
  key+value bytes, each sample tagged with the topic

#### Scenario: Produce failure records an error

- **WHEN** a `writer.produce` call fails
- **THEN** `kafka_writer_error_count` increases

#### Scenario: Metrics require the VU context

- **WHEN** metrics would be emitted outside a VU (no sample buffer available)
- **THEN** no sample is pushed and no panic occurs

### Requirement: Reader emits consume metrics

A `Reader` SHALL emit custom k6 metrics for its consume activity, registered on
the k6 metrics registry and pushed to the VU sample buffer, using the community
`mostafa/xk6-kafka` v1 metric names. Samples SHALL carry a `topic` tag when a
topic is known.

The Reader metrics are: `kafka_reader_message_count`,
`kafka_reader_message_bytes`, `kafka_reader_fetches_count`,
`kafka_reader_error_count`, `kafka_reader_rebalance_count`,
`kafka_reader_timeouts_count`, `kafka_reader_dial_count` (counters);
`kafka_reader_fetch_seconds`, `kafka_reader_read_seconds`,
`kafka_reader_wait_seconds`, `kafka_reader_dial_seconds`,
`kafka_reader_fetch_bytes`, `kafka_reader_fetch_size`, `kafka_reader_offset`,
`kafka_reader_lag` (trends).

#### Scenario: Successful consume records message counts and bytes

- **WHEN** `reader.consume` returns N messages
- **THEN** `kafka_reader_message_count` increases by N and
  `kafka_reader_message_bytes` increases by the total key+value bytes, tagged
  with the topic

#### Scenario: Lag is derived per message

- **WHEN** a message is consumed at offset O from a partition with high
  watermark H
- **THEN** `kafka_reader_lag` records `H - O - 1` (never negative) and
  `kafka_reader_offset` records O

#### Scenario: Consume timeout records a timeout

- **WHEN** a `reader.consume` call times out before reaching its limit
- **THEN** `kafka_reader_timeouts_count` increases

### Requirement: Omitted community metrics are documented, not emitted

The extension MUST document, rather than emit, community metrics that derive
from the `segmentio/kafka-go` stats structs and have no `twmb/franz-go`
equivalent. Such metrics SHALL be absent from the summary (not emitted as zero
or faked), and their absence SHALL be recorded in the compatibility matrix.

#### Scenario: Queue gauges are absent

- **WHEN** a test inspects the summary for `kafka_reader_queue_length` or
  `kafka_reader_queue_capacity`
- **THEN** those metrics are absent (documented as unsupported), and their
  absence does not fail the run
