## 1. Metrics collector scaffold

- [ ] 1.1 Add `pkg/kafka/metrics.go` with a `metricsCollector` that registers the Writer and Reader metric handles on the k6 metrics registry (get-or-create), obtained from the module instance
- [ ] 1.2 Add atomic accumulators for hook-sourced values (bytes written/read, dial count/seconds, e2e/batch seconds, batch size/bytes, retries, rebalances, fetches, timeouts)
- [ ] 1.3 Add a `flush` helper that builds `metrics.Sample`s (with a `topic` tag) and pushes them to `state.Samples`, no-op when `vu.State()` is nil

## 2. Writer metrics

- [ ] 2.1 Wire `writerHooks()` into the producer client options (`kgo.WithHooks`) for dial/bytes/batch events
- [ ] 2.2 In `Writer.produce`, count messages + serialized bytes and flush `kafka_writer_*` metrics (including error count on failure), tagged with the topic
- [ ] 2.3 Map retries via the retry hook into `kafka_writer_retries_count`

## 3. Reader metrics

- [ ] 3.1 Wire `readerHooks()` into the consumer client options for dial/bytes/fetch/rebalance events
- [ ] 3.2 In `Reader.consume`, count messages + bytes, compute `kafka_reader_lag = max(0, highWatermark-offset-1)` and `kafka_reader_offset` per message, and flush `kafka_reader_*` metrics tagged with the topic
- [ ] 3.3 Emit `kafka_reader_timeouts_count` on a consume timeout and `kafka_reader_error_count` on fetch errors

## 4. Tests

- [ ] 4.1 Unit test: collector registers all expected metric names with correct types on a fresh registry
- [ ] 4.2 Unit test: `flush` is a no-op with a nil VU state (no panic, no sample)
- [ ] 4.3 Unit test: lag derivation returns `max(0, highWatermark-offset-1)`
- [ ] 4.4 Integration: assert `kafka_writer_message_count` and `kafka_reader_message_count` appear in a produce/consume round-trip (add a threshold on them)

## 5. Docs

- [ ] 5.1 Document the supported `kafka_writer_*` / `kafka_reader_*` metrics and the explicitly omitted ones (`kafka_reader_queue_length`, `kafka_reader_queue_capacity`, config-echo gauges) in the compatibility matrix (issue #70)
- [ ] 5.2 Note in README that metrics are franz-go-derived and approximate for batch-granular trends
