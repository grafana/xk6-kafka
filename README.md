# xk6-kafka

> [!WARNING]
> **Early development.** Not yet released — the API below is the planned surface and may change.

`grafana/xk6-kafka` is the official, Grafana-owned, pure-Go [k6 extension](https://grafana.com/docs/k6/latest/extensions/) for load testing [Apache Kafka](https://kafka.apache.org): producing and consuming messages, managing topics, authenticating, and working with Schema Registry.

It is designed as a **100% pure-Go** extension (`CGO_ENABLED=0`): no C toolchain, no `confluentinc/librdkafka`, so it can run in lightweight containers, strict CI/CD pipelines, and Grafana Cloud k6.

## Goals

- **Official Grafana support** — owned and maintained by Grafana, with a clear path for security patches and releases.
- **Pure Go** — compiles without CGO, so it fits scratch containers, cross-compilation, and pure-Go build pipelines.
- **Familiar API** — aims to be a near-drop-in replacement for community [`mostafa/xk6-kafka`](https://github.com/mostafa/xk6-kafka) v1 scripts: same import and API shape, so common producer, consumer, admin, auth, and Schema Registry scripts run with little or no change.

See [RATIONALE.md](RATIONALE.md) for the problem, goals, and scope. Documentation and examples will land with the first release.

## Build

Use [xk6](https://github.com/grafana/xk6) to build a k6 binary with the extension:

```bash
xk6 build --with github.com/grafana/xk6-kafka
```

This produces a `k6` binary in the current directory. No C toolchain required. Until the first release, this builds from the latest `main` and may be incomplete.

## Usage

> [!NOTE]
> The example uses the planned v1-compatible API and is subject to change before release.

```javascript
import { Writer, Reader } from "k6/x/kafka";

const brokers = ["localhost:9092"];
const topic = "my-topic";

const writer = new Writer({ brokers, topic, autoCreateTopic: true });
const reader = new Reader({ brokers, topic });

export default function () {
  writer.produce({
    // strings are sent as UTF-8 bytes; pass a Uint8Array for raw bytes
    messages: [{ key: "key", value: "value" }],
  });

  const messages = reader.consume({ limit: 10 });
  console.log(messages);
}

export function teardown() {
  writer.close();
  reader.close();
}
```

Run it with the binary you built:

```bash
./k6 run script.js
```

## Compatibility

This extension aims for **familiarity, not a guarantee**: most community v1 scripts are expected to run with little or no change, but identical behavior is not promised. Some legacy tuning options have no pure-Go equivalent and are accepted but ignored, so behavior can differ in edge cases. Users who need behavior-identical, zero-change continuity should stay on `mostafa/xk6-kafka`.

A migration guide, a compatibility matrix, and known gaps for the main producer, consumer, admin, auth, and Schema Registry workflows will be published with the first release.

## Acknowledgments

This extension's API and design draw on the community [`mostafa/xk6-kafka`](https://github.com/mostafa/xk6-kafka) project by [Mostafa Moradian](https://github.com/mostafa). Thanks to Mostafa and its contributors for years of maintaining Kafka load testing in k6 and shaping the API that users know today.

This is not a takeover of the community extension, which continues independently. The aim is a separate, pure-Go reimplementation that Grafana can officially support and offer in Grafana Cloud k6. Users happy with `mostafa/xk6-kafka` can keep using it.

## License

Licensed under the [AGPL-3.0](LICENSE) license, the same license as [k6](https://github.com/grafana/k6).
