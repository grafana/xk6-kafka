// Integration test: topic administration against a real broker.
//
// create → list (present) → delete. The delete only asserts the broker accepts
// the request; Kafka removes the topic asynchronously, so this does not poll for
// the topic to disappear. Skips when KAFKA_BROKER is not configured (local/dev).
// In CI the broker address is set to the Kafka service, so failures here fail
// the test (they do not skip).
import { Connection } from "k6/x/kafka";

const broker = __ENV.KAFKA_BROKER;

export default function () {
  if (!broker) {
    console.log("KAFKA_BROKER not set; skipping admin integration test");
    return;
  }

  const topic = `xk6_admin_${Date.now()}`;
  const connection = new Connection({ address: broker });

  try {
    connection.createTopic({ topic, numPartitions: 1, replicationFactor: 1 });

    const after = connection.listTopics();
    if (!after.includes(topic)) {
      throw new Error(`created topic ${topic} not found in listTopics()`);
    }

    connection.deleteTopic(topic);
  } finally {
    connection.close();
  }
}
