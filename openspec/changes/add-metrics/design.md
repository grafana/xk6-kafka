## Context

k6 extensions surface custom metrics by registering them on the k6 metrics
registry (from the module init environment) and pushing `metrics.Sample`s to the
VU's sample buffer (`state.Samples`) during VU execution. The community
`mostafa/xk6-kafka` v1 exposes `kafka_writer_*` / `kafka_reader_*` metrics that
users assert on in `thresholds`; this extension emits none. Those metrics came
straight from `segmentio/kafka-go`'s `WriterStats` / `ReaderStats`; franz-go
does not expose the same struct, but its hook interfaces (`kgo.WithHooks`) cover
bytes on the wire, broker connects, request latency (E2E), and produce/fetch
batch events. Message-level counts are cleanest at the produce/consume call
sites, which already run on the VU goroutine.

## Goals / Non-Goals

**Goals:**
- Emit the mappable community `kafka_writer_*` / `kafka_reader_*` metrics with
  the same names and sensible counter/trend types, tagged by `topic`.
- Keep sample emission on the VU goroutine (correct k6 semantics).
- Reuse one collector design across Writer and Reader.

**Non-Goals:**
- 1:1 numeric parity with segmentio-derived metrics (different client).
- Emitting metrics with no franz-go source (`kafka_reader_queue_length`,
  `kafka_reader_queue_capacity`, config-echo gauges) — omitted and documented.
- Declaring metrics in `index.d.ts` (community does not; they are summary-only).

## Decisions

- **Hybrid collection: hooks accumulate, call sites emit.** franz-go invokes
  hooks from its own internal goroutines, where the VU context and
  `state.Samples` are not safe to touch. So hooks only update the collector's
  atomic counters/accumulators (bytes written/read, dials, dial/e2e durations,
  batch sizes, retries, rebalances). The Writer/Reader flush these into
  `metrics.Sample`s at the end of each `produce` / `consume` call — on the VU
  goroutine, where `state.Samples` and tags are valid. *Alternative:* push
  samples directly from hooks — rejected: wrong goroutine, no VU state, racy.

- **Message counts/bytes and lag come from the call site, not hooks.**
  `produce` knows the records and their serialized sizes; `consume` knows each
  record's offset and the partition high watermark, so
  `kafka_reader_lag = max(0, highWatermark - offset - 1)` is computed at decode.
  This avoids depending on hook batch granularity for the most-used metrics.

- **One `metricsCollector` type**, constructed from the k6 metrics registry,
  holding the registered `*metrics.Metric` handles plus atomic accumulators. It
  exposes `writerHooks()` / `readerHooks()` (the `kgo.Hook` set) added in
  `clientOptions`, and `flushProduce(...)` / `flushConsume(...)` called by
  Writer/Reader. Registry handles are created once per module instance.

- **Nil-VU safety.** Flushing checks `vu.State()`; when absent (init context) it
  skips emission rather than panicking, consistent with the produce/consume VU
  guards already in place.

- **Metric names/types mirror community v1** for familiarity: `*_count` /
  `*_bytes` as counters, `*_seconds` / `*_size` / `*_bytes`(per-batch) / `offset`
  / `lag` as trends. Names are the contract for compatibility even though the
  numbers are franz-go-derived.

## Risks / Trade-offs

- **Approximate trends (batch vs per-record).** franz-go reports batch-level
  write/fetch events; some `*_seconds` / `*_size` trends are batch-granular, not
  per-message. → Document as "franz-go-derived, approximate" in the compat
  matrix; keep the message counts exact (from call sites).
- **Hook goroutine races.** Mitigated by the accumulate-in-atomics /
  emit-at-call-site split; hooks never touch k6 state.
- **Per-VU vs shared client.** Each VU builds its own client and collector, so
  counters are per-VU; k6 aggregates across VUs in the summary, matching how
  community metrics behave. → No shared state needed.
- **Metric registration collisions.** Registering the same metric name twice on
  the k6 registry must reuse the existing handle. → Use the registry's
  get-or-create (`registry.GetOrNew` semantics), not blind `NewMetric`.

## Open Questions

- Exact franz-go hook → trend mapping for `*_wait_seconds` and
  `*_batch_seconds` (which hook timestamps best approximate segmentio's
  semantics) — resolved during implementation against the franz-go hook API.
- Whether to split delivery into two PRs (producer-metrics, consumer-metrics)
  sharing the collector, or ship as one — decided in tasks.
