# FM fact payloads

These files snapshot the durable side of FM fact payloads.
which is stored in the `fm_fact.payload` JSONB column:

- `fm_fact_payload_schemas.json` — the JSON **schema** of each DE's payload
  type. These structures will change for any change to the corresponding
  payload type, regardless of backward compatibility. These are verified
  (or regenerated, using EXPECTORATE) by the `fact_payload_schemas_are_stable`
  test. If you see this test fail, it indicates you are responsible for
  verifying data migration to the new schema.
- `fm_fact_payload_samples.json` — one serialized **sample** per each
  payload variant, for all diagnosis engines. These can change with a changing
  schema, but they can also change if the schema is constant but the
  serialization changes (e.g., the "kind" tag changes, a date format changes,
  etc).

They are checked by the `payload_stability` tests in
`nexus/fm/src/diagnosis/mod.rs`. **If a test points you here, you changed the
serialized shape of a persisted fact payload: that is a data migration**.

To decide whether the change actually needs a migration, two tests read the
still-committed goldens and diagnose it:

- `fact_payload_schema_changes_need_migration` diffs the old schema against the
  current types, keyed on the `kind` tag, and lists each change in migration
  terms (backfill a new required field, scrub a removed one, re-encode a changed
  type or narrowed domain). Purely additive changes are not flagged.
- `fact_payload_samples_deserialize` re-parses the committed sample bytes with
  the current types: the authoritative "do old rows still load" check.

Before regenerating with `EXPECTORATE=overwrite cargo nextest run -p nexus-fm`,
run those (and read the "Evolving persisted fact payloads" guidance at the top
of `nexus/fm/src/diagnosis/mod.rs`) to decide whether the change is additive
(serde-compatible) or breaking (needs a JSONB data migration).
