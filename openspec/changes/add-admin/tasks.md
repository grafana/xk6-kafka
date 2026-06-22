## 1. Connection VU handle

- [ ] 1.1 Give `Connection` the VU handle; `openConnection` takes the VU. Construction-time `Ping` keeps its background context + `pingTimeout`
- [ ] 1.2 Add an init/closed guard helper for admin calls (`vu.State() == nil` → throw; `client == nil` → throw)

## 2. createTopic

- [ ] 2.1 Validate locally: reject an empty `topic`; reject a `replicaAssignments` entry with a negative or duplicate `partition` (throw before contacting the broker)
- [ ] 2.2 Build a `kmsg.CreateTopicsRequest`: `numPartitions`/`replicationFactor` default to 1; when `replicaAssignments` is set, force both to -1 and map each `{partition, replicas}`; map `configEntries` to topic configs
- [ ] 2.3 Issue via `client.Request`, convert the per-topic `ErrorCode` with `kerr.ErrorForCode`, throw on transport or per-topic error
- [ ] 2.4 Unit-test request building (defaults, replica-assignment override, config entries) and local validation (empty topic, negative/duplicate partition)

## 3. deleteTopic

- [ ] 3.1 Reject an empty `topic` locally; build a `kmsg.DeleteTopicsRequest` for the named topic (set both `TopicNames` and `Topics` for version portability); issue, check per-topic error, throw on failure
- [ ] 3.2 Unit-test request building and empty-topic rejection

## 4. listTopics

- [ ] 4.1 Issue a `kmsg.MetadataRequest` (no topics), filter internal topics, return non-nil topic names
- [ ] 4.2 Unit-test internal-topic filtering and name extraction

## 5. Wiring & lifecycle

- [ ] 5.1 Expose `createTopic`, `deleteTopic`, `listTopics` on the Connection object returned to JS (alongside `close`)
- [ ] 5.2 Unit-test method exposure and the init/closed guards

## 6. Integration

- [ ] 6.1 Add a create → list → delete integration test (k6 script) against `KAFKA_BROKER`; skip when unset

## 7. Contract

- [ ] 7.1 Refine `index.d.ts` docs only: `numPartitions`/`replicationFactor` default to 1; `replicaAssignments` overrides `replicationFactor`; `listTopics` excludes internal topics. No type-shape changes

## 8. Validate

- [ ] 8.1 `go test ./...`, `gosec ./...`, `make lint`, `xk6 lint`, `xk6 build` (`CGO_ENABLED=0`), and `make it` (skips without `KAFKA_BROKER`) pass
- [ ] 8.2 Run `openspec validate add-admin --strict` and fix any issues
