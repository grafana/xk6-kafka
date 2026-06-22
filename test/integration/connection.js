// Integration test: connect to a real broker and close.
//
// Skips when KAFKA_BROKER is not configured (local/dev). In CI the broker
// address is set to the Kafka service, so an unreachable broker makes
// `new Connection` throw and fails the test (it does not skip).
import { Connection } from "k6/x/kafka";

const broker = __ENV.KAFKA_BROKER;

export default function () {
  if (!broker) {
    console.log("KAFKA_BROKER not set; skipping connection integration test");
    return;
  }

  const connection = new Connection({ address: broker });
  connection.close();
}
