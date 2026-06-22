## 1. Producer client

- [ ] 1.1 Extend the shared client options with producer opts: default topic, compression, partitioner (balancer), requiredAcks, `maxAttempts` (both record-retries and unknown-topic-retries), `writeTimeout` (produce request timeout), `batchBytes`, `batchTimeout` (linger), `autoCreateTopic`
- [ ] 1.2 Map `compression` (gzip/snappy/lz4/zstd) to franz-go codecs; unit-test
- [ ] 1.3 Map `balancer` (round-robin, hash, murmur2, least-bytes) to partitioners; `BALANCER_CRC32` / custom fn fall back to the default; unit-test
- [ ] 1.4 Map `requiredAcks` (-1/0/1); unit-test

## 2. Message marshaling

- [ ] 2.1 Marshal a message to a record: `key`/`value` (string → UTF-8 bytes, `Uint8Array` → bytes), `headers` (object), per-message `topic`, `time`
- [ ] 2.2 Unit-test marshaling (string and byte-array key/value, headers, topic override)

## 3. Writer

- [ ] 3.1 Implement `new Writer(WriterConfig)` building the producer client; accepted-but-ignored options (`batchSize`, `readTimeout`, `connectLogger`, `BALANCER_CRC32`/custom balancer) do not error
- [ ] 3.2 Implement `writer.produce({ messages })` via `ProduceSync`; block until acknowledged; throw on error
- [ ] 3.3 Implement `writer.close()` (flush + close)
- [ ] 3.4 Unit-test construction and the close/method exposure

## 4. Integration

- [ ] 4.1 Add an integration test (k6 script): construct a Writer with `autoCreateTopic`, produce messages, assert no error; skip when `KAFKA_BROKER` is unset
- [ ] 4.2 Add a produce-error integration case: producing to a non-existent topic with `autoCreateTopic` false throws

## 5. Validate

- [ ] 5.1 `go test ./...`, `gosec ./...`, `make lint` (golangci-lint), `xk6 lint`, `xk6 build` (`CGO_ENABLED=0`), and `make it` (`xk6 test`; skips without `KAFKA_BROKER`) pass
- [ ] 5.2 Run `openspec validate add-producer --strict` and fix any issues
