## 1. Config

- [ ] 1.1 Add `Schema string` (`js:"schema"`) to the Go `SubjectNameConfig`

## 2. Record full name

- [ ] 2.1 Add a helper that parses `schema` via `sr.parsedAvro(...)` and returns the fully-qualified name from a named Avro schema (type-assert to `avro.NamedSchema`, which covers record/enum/fixed and exposes `FullName()`); return an error when parsing fails or the schema is not a named type

## 3. getSubjectName strategies

- [ ] 3.1 Rewrite `getSubjectName` to dispatch on strategy using the `topicNameStrategy`/`recordNameStrategy`/`topicRecordNameStrategy` constants: empty → TopicNameStrategy; TopicName → `{topic}-{element}`; RecordName → `{fullname}`; TopicRecordName → `{topic}-{fullname}`; unknown → error. Keep the existing `strings.ToLower(element)` normalization on the TopicName path (identical for the contract's lowercase `key`/`value`, but keeps behavior stable and avoids an unused `strings` import)
- [ ] 3.2 Record strategies require a non-empty `schema`: error when empty; propagate the full-name helper's error for non-Avro/unnamed schemas
- [ ] 3.3 `GetSubjectName` threads `config.Schema` through and returns the new errors

## 4. Tests

- [ ] 4.1 Unit: TopicNameStrategy key/value, and empty strategy defaults to TopicNameStrategy
- [ ] 4.2 Unit: TopicNameStrategy ignores `schema` — an empty (or arbitrary) schema still returns `{topic}-{element}` (guards against a stray global empty-schema check breaking TopicName)
- [ ] 4.3 Unit: RecordNameStrategy returns the namespace-qualified full name for an Avro record (`com.example.User`)
- [ ] 4.4 Unit: RecordNameStrategy on a record with no namespace returns the bare name `User` (no leading dot) — pins `FullName()` vs a manual `namespace + "." + name`
- [ ] 4.5 Unit: RecordNameStrategy also works for a non-record named Avro schema (enum or fixed) — pins the `avro.NamedSchema` behavior so it can't silently narrow to records
- [ ] 4.6 Unit: TopicRecordNameStrategy returns `{topic}-{fullname}`
- [ ] 4.7 Unit: an empty schema returns an error — table-driven over **both** record strategies (RecordName and TopicRecordName), so the guard isn't wired into only one
- [ ] 4.8 Unit: a non-Avro / unnamed schema (JSON object, or Avro primitive) returns an error — table-driven over **both** record strategies
- [ ] 4.9 Unit: an unknown non-empty strategy returns an error

## 5. Docs

- [ ] 5.1 Note in the README / compatibility section that RecordNameStrategy and TopicRecordNameStrategy are Avro-only in v1 (JSON/Protobuf record naming not supported), and that an unknown strategy errors
- [ ] 5.2 Call out the behavior change for migration: scripts that previously set `RECORD_NAME_STRATEGY` / `TOPIC_RECORD_NAME_STRATEGY` silently got `{topic}-{element}` and will now get the real record-name subject (or an error) — i.e. the registry subject a script targets can change
