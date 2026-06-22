// Integration test: produce then consume a round-trip.
//
// Skips when KAFKA_BROKER is not configured (local/dev). In CI the broker
// address is set, so a failure fails the test.
import { Writer, Reader, START_OFFSETS_FIRST_OFFSET } from "k6/x/kafka";

const broker = __ENV.KAFKA_BROKER;

// Decode a consumed key/value (bytes) to a string, tolerating array,
// Uint8Array, or ArrayBuffer representations.
function toStr(v) {
  if (v instanceof ArrayBuffer) {
    v = new Uint8Array(v);
  }
  return String.fromCharCode.apply(null, v);
}

export default function () {
  if (!broker) {
    console.log("KAFKA_BROKER not set; skipping consumer integration test");
    return;
  }

  const topic = `xk6_kafka_roundtrip_${Date.now()}`;

  const writer = new Writer({ brokers: [broker], topic, autoCreateTopic: true });
  writer.produce({
    messages: [
      { key: "k1", value: "hello" },
      { key: "k2", value: "world" },
    ],
  });
  writer.close();

  const reader = new Reader({
    brokers: [broker],
    topic,
    partition: 0,
    startOffset: START_OFFSETS_FIRST_OFFSET,
  });

  let got = [];
  for (let i = 0; i < 5 && got.length < 2; i++) {
    got = got.concat(reader.consume({ limit: 2, expectTimeout: true }));
  }
  reader.close();

  if (got.length < 2) {
    throw new Error(`expected 2 messages, got ${got.length}`);
  }
  const values = got.map((m) => toStr(m.value));
  if (values.indexOf("hello") === -1 || values.indexOf("world") === -1) {
    throw new Error("round-trip values mismatch: " + JSON.stringify(values));
  }
}
