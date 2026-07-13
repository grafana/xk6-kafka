## 1. Setup and Dependencies

- [ ] 1.1 Add `github.com/hamba/avro` dependency to go.mod (Avro codec)
- [ ] 1.2 Create `pkg/kafka/schema_registry.go` with SchemaRegistry struct
- [ ] 1.3 Implement SchemaRegistry constructor (config validation, HTTP client init, standalone mode)

## 2. Registry HTTP Client

- [ ] 2.1 Implement `getSchema(subject, version)` HTTP GET to `/subjects/{subject}/versions/{version}`
- [ ] 2.2 Implement `createSchema(subject, schema, schemaType)` HTTP POST to `/subjects/{subject}/versions`
- [ ] 2.3 Add basic auth header support (BasicAuth from config)
- [ ] 2.4 Add error handling and logging for registry calls

## 3. Wire Format Utilities

- [ ] 3.1 Implement `encodeWireFormat(schemaID)` → 5-byte magic envelope (0x00 + 4-byte big-endian ID)
- [ ] 3.2 Implement `decodeWireFormat(data)` → (schemaID, remainingBytes) tuple, validate magic byte
- [ ] 3.3 Add tests for wire format round-trip (encode/decode ID values 0, 1, MAX_INT32)

## 4. STRING and BYTES SerDes

- [ ] 4.1 Implement `serialize()` for SCHEMA_TYPE_STRING (UTF-8 encode)
- [ ] 4.2 Implement `deserialize()` for SCHEMA_TYPE_STRING (UTF-8 decode, error on invalid UTF-8)
- [ ] 4.3 Implement `serialize()` for SCHEMA_TYPE_BYTES (pass-through)
- [ ] 4.4 Implement `deserialize()` for SCHEMA_TYPE_BYTES (pass-through)
- [ ] 4.5 Unit tests: STRING round-trip (empty, ASCII, special chars), BYTES round-trip (empty, binary)

## 5. Avro SerDes

- [ ] 5.1 Implement `serialize()` for SCHEMA_TYPE_AVRO (hamba/avro encode + optional wire format)
- [ ] 5.2 Implement `deserialize()` for SCHEMA_TYPE_AVRO (detect wire format by schema.id, strip and verify, hamba/avro decode)
- [ ] 5.3 Unit tests: Avro round-trip (simple record, union, null), error cases (type mismatch, corrupted bytes)
- [ ] 5.4 Unit tests: wire format Avro (registry-backed round-trip, schema ID verification)

## 6. JSON SerDes

- [ ] 6.1 Implement `serialize()` for SCHEMA_TYPE_JSON (validate required fields, JSON encode + optional wire format)
- [ ] 6.2 Implement `deserialize()` for SCHEMA_TYPE_JSON (detect wire format by schema.id, strip and verify, JSON parse + basic validation)
- [ ] 6.3 Unit tests: JSON round-trip (simple object, nested, types), required field validation
- [ ] 6.4 Unit tests: error cases (missing required field, type mismatch, malformed JSON, corrupted bytes)
- [ ] 6.5 Unit tests: wire format JSON (registry-backed round-trip, schema ID verification)

## 7. Public API Integration

- [ ] 7.1 Implement `getSubjectName(topic, element, strategy, schema)` → subject name (TopicNameStrategy)
- [ ] 7.2 Wire SchemaRegistry into module exports (index.js / register.go, export class)
- [ ] 7.3 Export all constants: SCHEMA_TYPE_AVRO, SCHEMA_TYPE_JSON, SCHEMA_TYPE_STRING, SCHEMA_TYPE_BYTES, KEY, VALUE, TOPIC_NAME_STRATEGY
- [ ] 7.4 Verify index.d.ts types match implementation (SchemaRegistry, Container, Schema, etc.)

## 8. Integration Tests

- [ ] 8.1 Extend `compose.yaml`: add Confluent Schema Registry service (image: confluentinc/cp-schema-registry, port 8081, KAFKA_BROKERS=kafka:9092)
- [ ] 8.2 Update `Makefile` `broker-up` target: wire `SCHEMA_REGISTRY_URL=http://localhost:8081` env var
- [ ] 8.3 Update `test/integration/lib/common.js`: add `getSchemaRegistry()` helper (reads SCHEMA_REGISTRY_URL env var)
- [ ] 8.4 Create `test/integration/schema-registry-avro.js` (register schema, round-trip produce/consume with Avro)
- [ ] 8.5 Create `test/integration/schema-registry-json.js` (register schema, round-trip produce/consume with JSON)
- [ ] 8.6 Create `test/integration/schema-registry-standalone.js` (inline schemas, no registry calls)
- [ ] 8.7 Create `test/integration/schema-registry-string-bytes.js` (STRING/BYTES round-trip, no registry)
- [ ] 8.8 Run `make integration` and confirm all tests pass

## 9. Documentation and Review

- [ ] 9.1 Add migration notes to README.md (how to port community v1 schema-registry scripts)
- [ ] 9.2 Document known limitations (no caching v1, no complex refs, no Protobuf)
- [ ] 9.3 Code review checklist: wire format correctness, error handling, test coverage
- [ ] 9.4 Commit and push for PR review
