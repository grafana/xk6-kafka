// Integration test: produce messages to a broker.
//
// Skips when KAFKA_BROKER is not configured (local/dev). In CI the broker
// address is set, so a produce failure fails the test.
import { Writer } from "k6/x/kafka";

const broker = __ENV.KAFKA_BROKER;

export default function () {
  if (!broker) {
    console.log("KAFKA_BROKER not set; skipping producer integration test");
    return;
  }

  const topic = `xk6_kafka_producer_${Date.now()}`;
  const writer = new Writer({ brokers: [broker], topic, autoCreateTopic: true });

  writer.produce({
    messages: [
      { key: "k1", value: "v1", headers: { source: "xk6" } },
      { value: "no key needed" },
    ],
  });

  writer.close();
}
