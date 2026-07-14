## 1. Metrics collector scaffold

- [ ] 1.1 Add `pkg/kafka/metrics.go` with a `metricsCollector` that registers the Writer and Reader metric handles on the k6 metrics registry (get-or-create), obtained from the module instance
- [ ] 1.2 Add hook-sourced state: monotonic atomic counters (bytes, dial count, retries, fetches, rebalances, timeouts) plus mutex-guarded observation buffers for trend values (dial/e2e/batch/fetch durations, batch/fetch sizes and bytes), each keyed by topic where the hook provides one
- [ ] 1.3 Implement delta emission: for counters, flush `total - lastFlushed` and advance `lastFlushed`; for trends, drain the buffered observations (one sample per value); guard the whole flush as a no-op when `vu.State()` is nil
- [ ] 1.4 Emit per-topic samples with a `topic` tag for topic-scoped metrics; emit connection/group-level metrics (`*_dial_*`, `rebalance`) untagged

## 2. Writer metrics

- [ ] 2.1 Wire `writerHooks()` into the producer client options (`kgo.WithHooks`) for dial and produce-batch events, bucketing batch trends by the hook-provided topic
- [ ] 2.2 In `Writer.produce`, bucket messages by topic (honoring per-message `Topic`), count messages + serialized bytes per topic, and flush `kafka_writer_*` (including `error_count` on failure)
- [ ] 2.3 Map retries into `kafka_writer_retries_count`

## 3. Reader metrics

- [ ] 3.1 Wire `readerHooks()` into the consumer client options for dial, fetch-batch, and group-rebalance events, bucketing fetch trends by topic
- [ ] 3.2 In `Reader.consume`, bucket returned messages by their own `Topic`; per topic count messages + bytes and record `kafka_reader_lag = max(0, highWatermark-offset-1)` and `kafka_reader_offset`; flush `kafka_reader_*`
- [ ] 3.3 Emit `kafka_reader_timeouts_count` on a consume timeout and `kafka_reader_error_count` on fetch errors

## 4. Tests

- [ ] 4.1 Unit: collector registers all expected metric names with the correct k6 types (Counter/Trend) on a fresh registry
- [ ] 4.2 Unit: `flush` is a no-op with a nil VU state (no panic, no sample)
- [ ] 4.3 Unit: hook-sourced counters emit **deltas** — two successive flushes of a rising total produce samples that sum to the total, not to an inflated value
- [ ] 4.4 Unit: lag derivation returns `max(0, highWatermark-offset-1)`
- [ ] 4.5 Unit: per-topic bucketing — a mixed-topic message set yields one sample set per topic, tagged correctly; connection/group metrics emit untagged
- [ ] 4.6 Integration: a produce/consume round-trip emits `kafka_writer_message_count` / `kafka_reader_message_count` with a `topic` tag (threshold on both); assert an omitted metric (`kafka_reader_queue_length`) is absent

## 5. Docs (self-contained in this change)

- [ ] 5.1 Add a README "Metrics" section listing the supported `kafka_writer_*` / `kafka_reader_*` metrics (name, type, tags) and the explicitly omitted ones (`kafka_reader_queue_length`, `kafka_reader_queue_capacity`, config-echo gauges) — so this change satisfies its own documentation requirement without depending on the compatibility-matrix issue (#70)
- [ ] 5.2 Note that metrics are franz-go-derived and that batch-granular trends are approximate vs the community's segmentio-derived values
