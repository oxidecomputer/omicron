# FM fact payload guardrail goldens

These files snapshot the persisted representation of every fault-management
diagnosis engine (DE) fact payload, which is serialized into the opaque
`fm_fact.payload` JSONB column:

- `fm_fact_payload_schemas.json` — the JSON **schema** of each DE's payload type
  (structure: variants, fields, types, and embedded-enum domains). Human-readable
  `description` text is stripped before snapshotting, so editing a doc comment
  (here or on an embedded type) does not register as a shape change.
- `fm_fact_payload_samples.json` — one obviously-fake serialized **sample** per
  payload variant (the exact on-disk bytes).

They are checked by the `payload_stability` tests in
`nexus/fm/src/diagnosis/mod.rs`. **If a test points you here, you changed the
serialized shape of a persisted fact payload — that is a data migration**, not
just a code change, because deployed databases already hold rows in the old
shape.

Before regenerating with `EXPECTORATE=overwrite cargo nextest run -p nexus-fm`,
read the "Evolving persisted fact payloads" guidance at the top of
`nexus/fm/src/diagnosis/mod.rs` to decide whether the change is additive
(serde-compatible) or breaking (needs a JSONB data migration).
