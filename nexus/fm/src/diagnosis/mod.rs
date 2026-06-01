// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management diagnosis engines.
//!
//! Each submodule defines one diagnosis engine (DE). `analyze` dispatches to
//! each engine in turn; engines are deterministic and idempotent per RFD 603,
//! so the dispatch order does not matter.
//!
//! # Evolving persisted fact payloads
//!
//! Each DE owns a fact-payload type serialized into the opaque `fm_fact.payload`
//! JSONB column (the owning DE of a fact is found via `fm_case.de`).
//! The `payload_stability` tests in this module snapshot every DE's payload (a
//! structural JSON-schema snapshot plus per-variant data samples) and fail
//! loudly when the representation changes.
//!
//! ## Is my change a data migration?
//!
//! A change is **backward-compatible** if and only if every value already on
//! disk still deserializes under the new type. Otherwise it is **breaking** and
//! existing rows must be rewritten.
//!
//! Examples:
//! - **Backward-compatible**: a new `#[serde(tag = "kind")]` variant; a new
//!   `Option<T>` or `#[serde(default)]` field; *widening* a field's domain.
//!   Importantly, old data can still be parsed.
//! - **Breaking**: renaming or removing a field; changing a field's type or
//!   value encoding; *narrowing* a domain; renaming or removing an enum variant
//!   that may exist in old rows; changing the `kind` tag.
//!
//! ## Writing the migration
//!
//! Rewrite existing rows with a data-only `UPDATE` in a new
//! `schema/crdb/<name>/up1.sql`. As a worked example, renaming the
//! `last_seen_health` key to `health` in the disk DE's `zpool_unhealthy`
//! payload:
//!
//! ```sql
//! UPDATE omicron.public.fm_fact
//! SET payload =
//!     -- Change the value of the payload
//!     jsonb_set(
//!         payload,
//!         -- INSERT the new 'health' field name.
//!         ARRAY['health'],
//!         -- Use the value from the old field
//!         payload->'last_seen_health'
//!     )
//!     -- REMOVE the old field
//!     - 'last_seen_health'
//! -- Scope this change to be specific to this DE, and specific to
//! -- this particular variant.
//! WHERE case_id IN (
//!     SELECT id FROM omicron.public.fm_case WHERE de = 'physical_disk'
//! )
//! AND payload->>'kind' = 'zpool_unhealthy'
//! -- For idempotency: Only run this query if the payload contains the
//! -- old value.
//! AND payload ? 'last_seen_health';
//! ```
//!
//! Then do the usual schema-version bookkeeping: add the `schema/crdb/<name>`
//! directory, bump `KNOWN_VERSIONS` and `dbinit.sql` (see
//! `schema/crdb/README.adoc`) — even though, from CockroachDB's point of view,
//! the column is unchanged JSONB and only the data changes — add a case to
//! `validate_data_migration()` in `nexus/tests/integration_tests/schema.rs`, and
//! regenerate the goldens.
//!
//! Prefer the additive route when you can: rather than rewriting an existing
//! variant in place, add a *new* `kind` variant for the new shape and migrate old
//! rows to it. The old shape stays readable throughout, which matters because
//! migrations here are offline and all-at-once.

use crate::SitrepBuilder;
use crate::analysis_input::Input;

mod physical_disk;

pub fn analyze(
    input: &Input,
    builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    physical_disk::analyze(input, builder)?;
    Ok(())
}

/// Ereport classes that any diagnosis engine in this build of Nexus knows
/// how to consume. The background task uses this to filter loaded ereports
/// — there is no value in loading ereports FM analysis cannot consume.
///
/// Empty today: the only enabled DE is the physical disk DE, which is
/// polling-based and consumes no ereports. Grow this list alongside FM
/// analysis as new classes gain ereport support.
///
/// **NULL-class ereports are intentionally excluded by the loader's SQL
/// filter** (`class = ANY(...)` never matches NULL). If FM analysis ever
/// needs to handle the "couldn't extract a class" or "reporter doesn't know
/// its identity" cases, that's an explicit decision (e.g. a sentinel
/// loader path), not a default of this list.
///
/// # Scaling
///
/// The loader filters ereports via `WHERE class = ANY($1::text[])` against
/// the existing `lookup_ereports_by_class` index. This is comfortable up to
/// a few hundred entries; past ~1000 entries, prefer either prefix matching
/// (`class LIKE 'ereport.cpu.amd.%'`) or a `known_ereport_class` lookup
/// table joined into the query. Revisit this if the list grows that large.
pub fn known_ereport_classes() -> &'static [&'static str] {
    &[]
}

/// A diagnosis engine's persisted fact-payload type.
#[cfg(test)]
trait FactPayload:
    serde::Serialize + serde::de::DeserializeOwned + schemars::JsonSchema + Sized
{
    /// The engine that owns this payload.
    const DE: nexus_types::fm::DiagnosisEngineKind;

    /// One sample per variant.
    ///
    /// Implementations should `match` exhaustively over their
    /// variants, so adding a variant forces a sample to be added here
    /// (otherwise it fails to compile).
    fn samples() -> Vec<Self>;
}

/// Guardrail tests snapshotting the serialized representation of every persisted
/// DE fact payload.
#[cfg(test)]
mod payload_stability {
    use super::FactPayload;
    use nexus_types::fm::DiagnosisEngineKind;
    use std::collections::BTreeMap;

    /// Deserializes an on-disk payload back into a DE's payload type `T` and
    /// returns its re-serialized form (for round-trip comparison), or the parse
    /// error if it no longer fits `T`. Captured while `T` is still known so the
    /// round-trip guardrail stays type-erased and DE-agnostic.
    type PayloadDeserializer = Box<
        dyn Fn(
            serde_json::Value,
        ) -> Result<serde_json::Value, serde_json::Error>,
    >;

    /// The type-erased snapshot of one DE's payload, produced by
    /// `snapshot_payload`. Erasing `T` lets `registered` collect heterogeneous
    /// DE payload types into one `Vec`.
    struct FactPayloadSnapshot {
        de: DiagnosisEngineKind,
        schema: schemars::schema::RootSchema,
        samples: Vec<FactPayloadSample>,
        /// Parses on-disk bytes back into this DE's payload type. See
        /// `fact_payload_samples_deserialize`.
        deserialize: PayloadDeserializer,
    }

    /// One obviously-fake serialized sample of a single payload variant.
    struct FactPayloadSample {
        /// The serde `kind` tag this variant serializes to: golden key + label.
        variant: String,
        /// The variant serialized to its on-disk JSON form.
        json: serde_json::Value,
    }

    /// Strips human-readable `description` text from every node of a schema, so
    /// the schema goldens and the migration diff react to structure, not prose.
    struct StripDescriptions;
    impl schemars::visit::Visitor for StripDescriptions {
        fn visit_schema_object(
            &mut self,
            schema: &mut schemars::schema::SchemaObject,
        ) {
            if let Some(metadata) = schema.metadata.as_mut() {
                metadata.description = None;
            }
            // Recurse into subschemas (properties, definitions, `oneOf`, ...).
            schemars::visit::visit_schema_object(self, schema);
        }
    }

    /// Snapshot one DE's payload type `T`: its schema and the serialized bytes
    /// of each sample, plus a deserializer for the round-trip guardrail. Fully
    /// generic, so no DE-specific logic lives here.
    fn snapshot_payload<T: FactPayload>() -> FactPayloadSnapshot {
        use schemars::visit::Visitor;

        let samples = T::samples()
            .into_iter()
            .map(|fact| {
                let json = serde_json::to_value(&fact).unwrap();
                // Label the sample by the serde `kind` tag it serializes to,
                // i.e. the bytes that land in `fm_fact.payload`.
                let variant = json
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .expect(
                        "fact payload must serialize with a string `kind` tag",
                    )
                    .to_string();
                FactPayloadSample { variant, json }
            })
            .collect();
        let mut schema = schemars::schema_for!(T);
        StripDescriptions.visit_root_schema(&mut schema);
        FactPayloadSnapshot {
            de: T::DE,
            schema,
            samples,
            // Closes over `T` only as a type (captures nothing), so the boxed
            // closure is `'static`.
            deserialize: Box::new(|json| {
                let typed: T = serde_json::from_value(json)?;
                serde_json::to_value(&typed)
            }),
        }
    }

    /// The single source of truth for which DEs the guardrail covers, and the
    /// *only* place that names individual DE payload types: adding a DE is one
    /// line here, and changing an existing DE's payload touches only that DE's
    /// own module (its `FactPayload` impl).
    fn registered() -> Vec<FactPayloadSnapshot> {
        vec![
            snapshot_payload::<super::physical_disk::DiskFact>(),
            // FUTURE DEs: snapshot_payload::<super::<de>::OtherFact>(),
        ]
    }

    /// Every `DiagnosisEngineKind` must either register a fact payload in
    /// `registered` or be listed in `NO_PAYLOAD`. Because this iterates the enum
    /// (rather than naming kinds by hand), a newly added DE kind that does
    /// neither fails this test, instead of silently going uncovered by the
    /// guardrail.
    #[test]
    fn every_de_kind_is_registered_or_exempt() {
        use strum::IntoEnumIterator;

        // DE kinds that intentionally have no persisted fact payload (yet). A
        // kind belongs here only if it writes no facts; once it gains a payload,
        // register it above and remove it from this list.
        const NO_PAYLOAD: &[DiagnosisEngineKind] =
            &[DiagnosisEngineKind::PowerShelf];

        let registered_des: Vec<DiagnosisEngineKind> =
            registered().iter().map(|d| d.de).collect();

        for de in DiagnosisEngineKind::iter() {
            let is_registered = registered_des.contains(&de);
            let is_exempt = NO_PAYLOAD.contains(&de);
            match (is_registered, is_exempt) {
                (true, false) | (false, true) => {}
                (false, false) => panic!(
                    "diagnosis engine `{de}` has no registered fact payload and \
                     is not in NO_PAYLOAD: register it in `registered`, or — if \
                     it has no persisted facts — add it to the NO_PAYLOAD \
                     exemption list",
                ),
                (true, true) => panic!(
                    "diagnosis engine `{de}` is both registered and in \
                     NO_PAYLOAD: remove it from NO_PAYLOAD",
                ),
            }
        }
    }

    /// Printed after a golden mismatch to head off the reflexive
    /// `EXPECTORATE=overwrite`: regenerating is only safe once you know the
    /// change needs no data migration or has migrated explicitly.
    const REGEN_GUIDANCE: &str = "\n\
========================================================================\n\
A persisted FM fact payload representation changed (see the diff above).\n\
\n\
Do NOT regenerate the golden yet. First decide whether this is a data\n\
migration, using the goldens still on disk:\n\
\n\
  1. Run the migration guardrails:\n\
       # Did the schema change in a way that demands migration? \n\
       cargo nextest run -p nexus-fm fact_payload_schema_changes_need_migration\n\
\n\
       # Do the samples still deserialize? \n\
       cargo nextest run -p nexus-fm fact_payload_samples_deserialize\n\
\n\
  2. If either FAILS, a data migration is required (the first test names each\n\
     change and why). Write the migration first (see the 'Evolving persisted\n\
     fact payloads' docs at the top of nexus/fm/src/diagnosis/mod.rs), then\n\
     regenerate.\n\
\n\
  3. If both PASS, no data migration is needed. Regenerate the golden:\n\
       EXPECTORATE=overwrite cargo nextest run -p nexus-fm payload_stability\n\
========================================================================";

    /// `expectorate::assert_contents`, plus `guidance` printed *after*
    /// expectorate's diff if the assertion fails (so it lands as the last thing
    /// the developer sees, beneath the diff and `EXPECTORATE=overwrite` hint).
    fn assert_golden(relative_path: &str, contents: &str, guidance: &str) {
        let result = std::panic::catch_unwind(|| {
            expectorate::assert_contents(relative_path, contents);
        });
        if let Err(payload) = result {
            eprintln!("{guidance}");
            std::panic::resume_unwind(payload);
        }
    }

    /// Structural guardrail: snapshots the JSON schema of every DE payload. A
    /// diff here means a field/variant/type changed (including an embedded type
    /// owned by another crate). The committed schema is also the "old" side that
    /// `fact_payload_schema_changes_need_migration` diffs against.
    #[test]
    fn fact_payload_schemas_are_stable() {
        let serialized =
            serde_json::to_string_pretty(&schema_doc()).unwrap() + "\n";
        assert_golden(
            "output/fm_fact_payload_schemas.json",
            &serialized,
            REGEN_GUIDANCE,
        );
    }

    /// The schema golden, keyed by DE: one `RootSchema` per registered payload.
    fn schema_doc() -> BTreeMap<String, serde_json::Value> {
        let mut doc = BTreeMap::new();
        for d in registered() {
            if doc
                .insert(
                    d.de.to_string(),
                    serde_json::to_value(&d.schema).unwrap(),
                )
                .is_some()
            {
                panic!(
                    "diagnosis engine `{}` registered more than one fact payload \
                     type; we currently expect one fact type per DE. Consider \
                     using an enum variant.",
                    d.de,
                );
            }
        }
        doc
    }

    /// Migration guardrail: diffs the committed schema golden (the "old" shape
    /// rows on disk were written in) against the current payload types, and
    /// fails listing every change that warrants a data migration.
    ///
    /// Our payloads are `#[serde(tag = "kind")]` enums, so the diff is keyed on
    /// the `kind` tag: it walks each variant and reports field- and
    /// variant-level changes in migration terms (backfill / scrub / re-encode).
    /// `$ref`s are inlined first, so a change to an embedded type (e.g. a
    /// narrowed `ZpoolHealth` domain) surfaces as a change to the field that
    /// uses it.
    ///
    /// "Warrants a migration" is broader than "fails to deserialize": removing a
    /// field still deserializes (serde ignores the now-dead key), but you want a
    /// migration to scrub it, so it is flagged. Purely additive changes (a new
    /// optional field, a new variant, a field that became optional) do not need
    /// one and are not flagged.
    ///
    /// Like the round-trip guardrail, this reads the *committed* golden, so it
    /// reports changes relative to the last commit; regenerating the golden
    /// resets the baseline. Run it (and write any migration) *before* you
    /// regenerate.
    #[test]
    fn fact_payload_schema_changes_need_migration() {
        let old: BTreeMap<String, serde_json::Value> = serde_json::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/output/fm_fact_payload_schemas.json"
            ))
            .expect(
                "golden schema file should exist; run \
                 fact_payload_schemas_are_stable to generate it",
            ),
        )
        .expect("golden schema file should be a JSON object of schemas");
        let new = schema_doc();

        let mut findings = Vec::new();
        for de in old.keys() {
            if !new.contains_key(de) {
                findings.push(format!(
                    "DE `{de}` no longer registers a payload: existing \
                     `{de}` rows are orphaned and need migrating or deleting. \
                     Otherwise, they could exist in sitreps and be unparseable!",
                ));
            }
        }
        for (de, new_schema) in &new {
            // A DE absent from `old` is newly added: no rows predate it.
            if let Some(old_schema) = old.get(de) {
                findings.extend(
                    diff_payload(de, old_schema, new_schema)
                        .iter()
                        .map(|f| f.describe(de)),
                );
            }
        }

        assert!(
            findings.is_empty(),
            "fact payload schema changed in ways that need a data migration \
             (see the 'Evolving persisted fact payloads' docs at the top of \
             nexus/fm/src/diagnosis/mod.rs):\n{}",
            findings
                .iter()
                .map(|f| format!("  - {f}"))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }

    /// A payload variant reduced to what the migration diff compares: each
    /// field's required-ness and its `$ref`-inlined, description-free schema.
    type Variant = BTreeMap<String, Field>;
    struct Field {
        required: bool,
        schema: serde_json::Value,
    }

    /// Breaks one DE's `RootSchema` JSON into its `kind` variants. Keys are the
    /// serde tag values (the `kind` enum literal of each `oneOf` branch).
    fn variants(
        context: &str,
        root: &serde_json::Value,
    ) -> BTreeMap<String, Variant> {
        let definitions = root.get("definitions");
        let branches =
            root.get("oneOf").and_then(|o| o.as_array()).unwrap_or_else(|| {
                panic!(
                    "{context}: payload schema is not a `kind`-tagged enum (no \
                     top-level `oneOf`). The payload-stability guardrails only \
                     support `#[serde(tag = \"kind\")]` enums; model the \
                     payload that way, or teach `variants` to handle this \
                     shape."
                )
            });
        let mut out = BTreeMap::new();
        for branch in branches {
            let props = branch
                .get("properties")
                .and_then(|p| p.as_object())
                .unwrap_or_else(|| {
                    panic!(
                        "{context}: a payload variant did not render as a flat \
                         object with `properties` (it is a `$ref`, or composed \
                         with `allOf`/`anyOf`). `variants` only understands \
                         inlined variant objects; teach it this shape or \
                         simplify the payload."
                    )
                });
            let required: std::collections::BTreeSet<&str> = branch
                .get("required")
                .and_then(|r| r.as_array())
                .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
            let kind = props
                .get("kind")
                .and_then(|k| k.get("enum"))
                .and_then(|e| e.as_array())
                .and_then(|e| e.first())
                .and_then(|k| k.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "{context}: a payload variant has no string `kind` enum \
                         literal. The serde tag must be named `kind` and render \
                         as a single-value enum; check the \
                         `#[serde(tag = ...)]` attribute."
                    )
                })
                .to_string();
            let fields = props
                .iter()
                .filter(|(name, _)| name.as_str() != "kind")
                .map(|(name, schema)| {
                    (
                        name.clone(),
                        Field {
                            required: required.contains(name.as_str()),
                            schema: inline_refs(
                                schema,
                                definitions,
                                &mut Vec::new(),
                            ),
                        },
                    )
                })
                .collect();
            out.insert(kind, fields);
        }
        out
    }

    /// Replaces every `{"$ref": "#/definitions/X"}` with `X`'s (recursively
    /// inlined) schema, so a field's compared shape reflects the embedded type.
    /// `seen` guards against a type that refers back to itself.
    fn inline_refs(
        schema: &serde_json::Value,
        definitions: Option<&serde_json::Value>,
        seen: &mut Vec<String>,
    ) -> serde_json::Value {
        match schema {
            serde_json::Value::Object(map) => {
                if let Some(name) = map
                    .get("$ref")
                    .and_then(|r| r.as_str())
                    .and_then(|r| r.strip_prefix("#/definitions/"))
                {
                    if seen.iter().any(|s| s == name) {
                        return schema.clone(); // cycle: leave the ref in place
                    }
                    let target = definitions
                        .and_then(|d| d.get(name))
                        .unwrap_or_else(|| panic!("dangling $ref to `{name}`"));
                    seen.push(name.to_string());
                    let inlined = inline_refs(target, definitions, seen);
                    seen.pop();
                    return inlined;
                }
                serde_json::Value::Object(
                    map.iter()
                        .map(|(k, v)| {
                            (k.clone(), inline_refs(v, definitions, seen))
                        })
                        .collect(),
                )
            }
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .iter()
                    .map(|v| inline_refs(v, definitions, seen))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// A change that warrants a data migration, produced by [`diff_payload`].
    /// Holds the `kind` (and field) the change touches; the owning DE is paired
    /// in at formatting time via [`Finding::describe`], since `de` is caller
    /// context, not part of the change itself.
    #[derive(Debug, PartialEq, Eq)]
    enum Finding {
        /// A whole `kind` variant disappeared; rows of that kind are orphaned.
        VariantRemoved { kind: String },
        /// A new required field appeared; old rows lack it.
        AddedRequired { kind: String, field: String },
        /// An existing field went optional -> required; some rows lack it.
        BecameRequired { kind: String, field: String },
        /// An existing field's (`$ref`-inlined) schema changed: a type or
        /// domain change.
        SchemaChanged { kind: String, field: String },
        /// A field disappeared; old rows still carry the now-dead key.
        Removed { kind: String, field: String },
    }

    impl Finding {
        /// The human-readable migration message, scoped to its owning DE.
        fn describe(&self, de: &str) -> String {
            match self {
                Finding::VariantRemoved { kind } => format!(
                    "{de}: variant `{kind}` was removed: existing `{kind}` rows \
                     are orphaned and need migrating or deleting",
                ),
                Finding::AddedRequired { kind, field } => format!(
                    "{de}::{kind}: required field `{field}` was added: backfill \
                     it on existing rows",
                ),
                Finding::BecameRequired { kind, field } => format!(
                    "{de}::{kind}: field `{field}` became required: backfill it \
                     on rows that lack it",
                ),
                Finding::SchemaChanged { kind, field } => format!(
                    "{de}::{kind}: field `{field}` changed type or domain: \
                     re-encode or verify existing rows",
                ),
                Finding::Removed { kind, field } => format!(
                    "{de}::{kind}: field `{field}` was removed: scrub it from \
                     existing rows",
                ),
            }
        }
    }

    /// Diffs one DE's old vs new payload schema, returning a structured
    /// [`Finding`] for every change that warrants a data migration. `de` is
    /// used only for the `variants` panic context (a `Finding` does not carry
    /// it; the caller pairs it in via [`Finding::describe`]).
    fn diff_payload(
        de: &str,
        old_schema: &serde_json::Value,
        new_schema: &serde_json::Value,
    ) -> Vec<Finding> {
        let old =
            variants(&format!("DE `{de}` (committed golden)"), old_schema);
        let new = variants(&format!("DE `{de}` (current types)"), new_schema);

        let mut findings = Vec::new();
        for kind in old.keys() {
            if !new.contains_key(kind) {
                findings.push(Finding::VariantRemoved { kind: kind.clone() });
            }
        }

        // A variant present only in `new` is additive: old rows predate it.
        for (kind, new_fields) in &new {
            // If the whole variant is new, it doesn't exist in old data.
            let Some(old_fields) = old.get(kind) else { continue };

            // Otherwise, the variant exists in old and new schemas.
            //
            // Let's check for compatibility.
            for (name, new_field) in new_fields {
                match old_fields.get(name) {
                    None if new_field.required => {
                        findings.push(Finding::AddedRequired {
                            kind: kind.clone(),
                            field: name.clone(),
                        })
                    }
                    // A new optional field is fine: old rows deserialize without it.
                    None => {}
                    Some(old_field) => {
                        if !old_field.required && new_field.required {
                            findings.push(Finding::BecameRequired {
                                kind: kind.clone(),
                                field: name.clone(),
                            });
                        }
                        // required -> optional is fine: old rows already have it.
                        if old_field.schema != new_field.schema {
                            findings.push(Finding::SchemaChanged {
                                kind: kind.clone(),
                                field: name.clone(),
                            });
                        }
                    }
                }
            }
            for name in old_fields.keys() {
                if !new_fields.contains_key(name) {
                    findings.push(Finding::Removed {
                        kind: kind.clone(),
                        field: name.clone(),
                    });
                }
            }
        }
        findings
    }

    /// Sample stability guardrail: snapshots the exact on-disk JSON for one
    /// sample of every payload variant. A diff here means the serialized
    /// contents changed.
    #[test]
    fn fact_payload_samples_are_stable() {
        let mut doc = BTreeMap::new();
        for d in registered() {
            for sample in d.samples {
                let key = format!("{}::{}", d.de, sample.variant);
                if doc.insert(key.clone(), sample.json).is_some() {
                    panic!(
                        "duplicate fact payload sample key `{key}`: two \
                         variants serialized to the same `de::variant` label",
                    );
                }
            }
        }
        let serialized = serde_json::to_string_pretty(&doc).unwrap() + "\n";
        assert_golden(
            "output/fm_fact_payload_samples.json",
            &serialized,
            REGEN_GUIDANCE,
        );
    }

    /// Round-trip guardrail: every sample in the committed golden file must
    /// still deserialize into its DE's *current* payload type and re-serialize
    /// to the same bytes. `fact_payload_samples_are_stable` pins the serialize
    /// direction; this pins the deserialize direction, so the on-disk
    /// representation and the current type are proven to agree both ways.
    ///
    /// A breaking change (a removed/renamed field, a narrowed domain, a renamed
    /// variant) makes the committed golden fail to parse here, flagging that
    /// rows already on disk would no longer load and need a data migration.
    ///
    /// This reads the bytes actually on disk rather than the freshly generated
    /// samples, but it is *not* a frozen historical corpus: regenerating the
    /// golden also moves what is checked here. Its standing guarantee is
    /// round-trip symmetry of the current type; its migration value comes from
    /// running it (and reading the stability diff) *before* you regenerate.
    #[test]
    fn fact_payload_samples_deserialize() {
        // Map each `de::variant` golden key to the owning DE's deserializer.
        let registered = registered();
        let mut deserializers: BTreeMap<String, &PayloadDeserializer> =
            BTreeMap::new();
        for d in &registered {
            for sample in &d.samples {
                deserializers.insert(
                    format!("{}::{}", d.de, sample.variant),
                    &d.deserialize,
                );
            }
        }

        // Parse the bytes actually on disk, not the freshly generated samples.
        let golden = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/output/fm_fact_payload_samples.json"
        ))
        .expect(
            "golden sample file should exist; run \
             fact_payload_samples_are_stable to generate it",
        );
        let golden: BTreeMap<String, serde_json::Value> = serde_json::from_str(
            &golden,
        )
        .expect("golden sample file should be a JSON object of samples");

        for (key, expected) in golden {
            let deserialize = deserializers.get(&key).unwrap_or_else(|| {
                panic!(
                    "golden sample `{key}` matches no registered payload \
                     variant: a variant was renamed or removed. If rows on disk \
                     may still carry it, that is a breaking change needing a \
                     data migration (see the module docs); otherwise regenerate \
                     the golden.",
                )
            });
            let round_tripped =
                deserialize(expected.clone()).unwrap_or_else(|e| {
                    panic!(
                        "golden sample `{key}` no longer deserializes into the \
                         current payload type: {e}. This is a breaking change; \
                         rows on disk need a data migration (see the module \
                         docs).",
                    )
                });
            assert_eq!(
                round_tripped, expected,
                "golden sample `{key}` deserialized but re-serialized to \
                 different bytes: serialize and deserialize are asymmetric \
                 (e.g. a field is skipped on one side), which silently drops or \
                 rewrites persisted data",
            );
        }
    }

    /// Unit tests for `diff_payload` / `variants` on hand-built schemas, pinning
    /// the migration-classification claims the guardrail makes.
    mod diff_tests {
        use super::*;
        use serde_json::json;

        /// `(field name, required, type schema)`.
        type FieldSpec<'a> = (&'a str, bool, serde_json::Value);
        /// `(kind tag, its fields)`.
        type VariantSpec<'a> = (&'a str, &'a [FieldSpec<'a>]);

        /// Builds a `kind`-tagged-enum payload schema (the shape schemars
        /// emits) from per-variant specs.
        fn payload(variants: &[VariantSpec]) -> serde_json::Value {
            let branches: Vec<serde_json::Value> = variants
                .iter()
                .map(|&(kind, fields)| {
                    let mut props = serde_json::Map::new();
                    props.insert("kind".to_string(), json!({ "enum": [kind] }));
                    let mut required = vec![json!("kind")];
                    for (name, req, schema) in fields {
                        props.insert((*name).to_string(), schema.clone());
                        if *req {
                            required.push(json!(*name));
                        }
                    }
                    let mut branch = serde_json::Map::new();
                    branch.insert("type".to_string(), json!("object"));
                    branch.insert(
                        "properties".to_string(),
                        serde_json::Value::Object(props),
                    );
                    branch.insert(
                        "required".to_string(),
                        serde_json::Value::Array(required),
                    );
                    serde_json::Value::Object(branch)
                })
                .collect();
            let mut root = serde_json::Map::new();
            root.insert(
                "oneOf".to_string(),
                serde_json::Value::Array(branches),
            );
            serde_json::Value::Object(root)
        }

        fn diff(
            old: &serde_json::Value,
            new: &serde_json::Value,
        ) -> Vec<Finding> {
            diff_payload("de", old, new)
        }

        fn string() -> serde_json::Value {
            json!({ "type": "string" })
        }
        fn integer() -> serde_json::Value {
            json!({ "type": "integer" })
        }

        fn added_required(kind: &str, field: &str) -> Finding {
            Finding::AddedRequired { kind: kind.into(), field: field.into() }
        }
        fn removed(kind: &str, field: &str) -> Finding {
            Finding::Removed { kind: kind.into(), field: field.into() }
        }
        fn schema_changed(kind: &str, field: &str) -> Finding {
            Finding::SchemaChanged { kind: kind.into(), field: field.into() }
        }

        #[test]
        fn added_required_field_is_flagged() {
            let old = payload(&[("v", &[("a", true, string())])]);
            let new = payload(&[(
                "v",
                &[("a", true, string()), ("b", true, string())],
            )]);
            assert_eq!(diff(&old, &new), vec![added_required("v", "b")]);
        }

        #[test]
        fn added_optional_field_is_ignored() {
            let old = payload(&[("v", &[("a", true, string())])]);
            let new = payload(&[(
                "v",
                &[("a", true, string()), ("b", false, string())],
            )]);
            assert_eq!(diff(&old, &new), vec![]);
        }

        #[test]
        fn removed_field_is_flagged() {
            let old = payload(&[(
                "v",
                &[("a", true, string()), ("b", true, string())],
            )]);
            let new = payload(&[("v", &[("a", true, string())])]);
            assert_eq!(diff(&old, &new), vec![removed("v", "b")]);
        }

        #[test]
        fn optional_to_required_is_flagged() {
            let old = payload(&[("v", &[("a", false, string())])]);
            let new = payload(&[("v", &[("a", true, string())])]);
            assert_eq!(
                diff(&old, &new),
                vec![Finding::BecameRequired {
                    kind: "v".into(),
                    field: "a".into(),
                }],
            );
        }

        #[test]
        fn required_to_optional_is_ignored() {
            let old = payload(&[("v", &[("a", true, string())])]);
            let new = payload(&[("v", &[("a", false, string())])]);
            assert_eq!(diff(&old, &new), vec![]);
        }

        #[test]
        fn field_type_change_is_flagged() {
            let old = payload(&[("v", &[("a", true, string())])]);
            let new = payload(&[("v", &[("a", true, integer())])]);
            assert_eq!(diff(&old, &new), vec![schema_changed("v", "a")]);
        }

        #[test]
        fn became_required_and_type_changed_are_both_flagged() {
            let old = payload(&[("v", &[("a", false, string())])]);
            let new = payload(&[("v", &[("a", true, integer())])]);
            assert_eq!(
                diff(&old, &new),
                vec![
                    Finding::BecameRequired {
                        kind: "v".into(),
                        field: "a".into(),
                    },
                    schema_changed("v", "a"),
                ],
            );
        }

        #[test]
        fn embedded_ref_domain_change_surfaces() {
            // Field `a` is typed by a `$ref` whose definition's domain narrows.
            // Proves `inline_refs` lifts the embedded change onto the field.
            let with_domain = |values: serde_json::Value| {
                json!({
                    "oneOf": [{
                        "type": "object",
                        "properties": {
                            "kind": { "enum": ["v"] },
                            "a": { "$ref": "#/definitions/Health" },
                        },
                        "required": ["kind", "a"],
                    }],
                    "definitions": { "Health": { "enum": values } },
                })
            };
            let old = with_domain(json!(["ok", "bad", "ugly"]));
            let new = with_domain(json!(["ok", "bad"]));
            assert_eq!(diff(&old, &new), vec![schema_changed("v", "a")]);
        }

        #[test]
        fn recursive_type_terminates_and_diffs() {
            // A self-referential definition: `Node` contains a `Node`.
            // `inline_refs` must terminate (its `seen` guard cuts the cycle)
            // rather than recurse forever, and still surface a change inside the
            // recursive type at the top-level field that uses it.
            let tree = |value: serde_json::Value| {
                json!({
                    "oneOf": [{
                        "type": "object",
                        "properties": {
                            "kind": { "enum": ["v"] },
                            "root": { "$ref": "#/definitions/Node" },
                        },
                        "required": ["kind", "root"],
                    }],
                    "definitions": {
                        "Node": {
                            "type": "object",
                            "properties": {
                                "value": value,
                                "child": { "$ref": "#/definitions/Node" },
                            },
                        },
                    },
                })
            };
            let old = tree(string());
            // Comparing the cyclic schema to itself must terminate and be empty.
            assert_eq!(diff(&old, &old), vec![]);
            // A change inside the recursive type surfaces at the field using it.
            let new = tree(integer());
            assert_eq!(diff(&old, &new), vec![schema_changed("v", "root")]);
        }

        #[test]
        fn variant_removed_is_flagged() {
            let old = payload(&[
                ("v", &[("a", true, string())]),
                ("w", &[("b", true, string())]),
            ]);
            let new = payload(&[("v", &[("a", true, string())])]);
            assert_eq!(
                diff(&old, &new),
                vec![Finding::VariantRemoved { kind: "w".into() }],
            );
        }

        #[test]
        fn variant_added_is_ignored() {
            let old = payload(&[("v", &[("a", true, string())])]);
            let new = payload(&[
                ("v", &[("a", true, string())]),
                ("w", &[("b", true, string())]),
            ]);
            assert_eq!(diff(&old, &new), vec![]);
        }

        #[test]
        fn identical_schema_has_no_findings() {
            let old = payload(&[("v", &[("a", true, string())])]);
            assert_eq!(diff(&old, &old), vec![]);
        }

        #[test]
        fn rename_is_remove_plus_add() {
            // A serialized-key rename has no single "rename" finding; it shows
            // up as the add + remove pair that the migration must reconcile.
            let old = payload(&[("v", &[("old_name", true, string())])]);
            let new = payload(&[("v", &[("new_name", true, string())])]);
            assert_eq!(
                diff(&old, &new),
                vec![added_required("v", "new_name"), removed("v", "old_name")],
            );
        }

        #[test]
        fn multiple_changes_in_one_diff() {
            let old = payload(&[(
                "v",
                &[("a", true, string()), ("gone", true, string())],
            )]);
            let new = payload(&[(
                "v",
                &[("a", true, integer()), ("added", true, string())],
            )]);
            // Field order is deterministic: `variants` keys fields in a
            // `BTreeMap`, so `a` then `added`, then the removed pass for `gone`.
            assert_eq!(
                diff(&old, &new),
                vec![
                    schema_changed("v", "a"),
                    added_required("v", "added"),
                    removed("v", "gone"),
                ],
            );
        }

        #[test]
        fn describe_renders_each_finding() {
            assert_eq!(
                added_required("zpool_unhealthy", "severity")
                    .describe("physical_disk"),
                "physical_disk::zpool_unhealthy: required field `severity` was \
                 added: backfill it on existing rows",
            );
            assert_eq!(
                removed("zpool_unhealthy", "old").describe("physical_disk"),
                "physical_disk::zpool_unhealthy: field `old` was removed: scrub \
                 it from existing rows",
            );
            assert_eq!(
                schema_changed("zpool_unhealthy", "x")
                    .describe("physical_disk"),
                "physical_disk::zpool_unhealthy: field `x` changed type or \
                 domain: re-encode or verify existing rows",
            );
            assert_eq!(
                Finding::BecameRequired {
                    kind: "zpool_unhealthy".into(),
                    field: "x".into(),
                }
                .describe("physical_disk"),
                "physical_disk::zpool_unhealthy: field `x` became required: \
                 backfill it on rows that lack it",
            );
            assert_eq!(
                Finding::VariantRemoved { kind: "gone".into() }
                    .describe("physical_disk"),
                "physical_disk: variant `gone` was removed: existing `gone` \
                 rows are orphaned and need migrating or deleting",
            );
        }

        #[test]
        #[should_panic(expected = "no top-level")]
        fn non_enum_schema_panics() {
            variants(
                "ctx",
                &json!({
                    "type": "object",
                    "properties": { "a": { "type": "string" } },
                }),
            );
        }

        #[test]
        #[should_panic(expected = "did not render as a flat object")]
        fn variant_without_properties_panics() {
            variants(
                "ctx",
                &json!({ "oneOf": [ { "$ref": "#/definitions/X" } ] }),
            );
        }

        #[test]
        #[should_panic(expected = "`kind` enum literal")]
        fn variant_without_kind_panics() {
            variants(
                "ctx",
                &json!({
                    "oneOf": [{
                        "type": "object",
                        "properties": { "a": { "type": "string" } },
                    }],
                }),
            );
        }
    }
}
