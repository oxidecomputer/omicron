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
//! JSONB column (the owning DE of a fact is found via `fm_case.de`). Deployed
//! databases hold rows in whatever shape was current when they were written.
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
//! For a value *re-encoding* rather than a key rename, keep the same skeleton and
//! change only the new value, e.g. wrapping the bare string in an object:
//!
//! ```sql
//! SET payload = jsonb_set(
//!     payload,
//!     ARRAY['last_seen_health'],
//!     jsonb_build_object('state', payload->'last_seen_health')
//! )
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

/// A diagnosis engine's persisted fact-payload type. Each DE's payload enum
/// implements this so the payload-stability guardrail can snapshot it
/// generically: the DE only declares its engine kind and an obviously-fake
/// sample of every variant, and the guardrail derives the schema and the
/// serialized bytes from the type itself. The impl lives in the DE's own
/// module, so changing a payload stays self-contained there.
#[cfg(test)]
trait FactPayload: serde::Serialize + schemars::JsonSchema + Sized {
    /// The engine that owns this payload.
    const DE: nexus_types::fm::DiagnosisEngineKind;

    /// One obviously-fake sample per variant. The guardrail labels each sample
    /// by the serde `kind` tag it serializes to, so there is no separate name
    /// to keep in sync. Implementations should `match` exhaustively over their
    /// variants, so adding a variant forces a sample to be added here
    /// (otherwise it fails to compile).
    fn samples() -> Vec<Self>;
}

/// Guardrail tests snapshotting the serialized representation of every persisted
/// DE fact payload. See the module-level "Evolving persisted fact payloads"
/// docs above before regenerating any golden file: a diff here means a
/// persisted payload changed shape, which is a data migration.
#[cfg(test)]
mod payload_stability {
    use super::FactPayload;
    use nexus_types::fm::DiagnosisEngineKind;
    use std::collections::BTreeMap;

    /// The type-erased snapshot of one DE's payload, produced by
    /// `snapshot_payload`. Erasing `T` lets `registered` collect heterogeneous
    /// DE payload types into one `Vec`.
    struct FactPayloadSnapshot {
        de: DiagnosisEngineKind,
        schema: schemars::schema::RootSchema,
        samples: Vec<FactPayloadSample>,
    }

    /// One obviously-fake serialized sample of a single payload variant.
    struct FactPayloadSample {
        /// The serde `kind` tag this variant serializes to: golden key + label.
        variant: String,
        /// The variant serialized to its on-disk JSON form.
        json: serde_json::Value,
    }

    /// Strips human-readable `description` text from every node of a schema.
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
    /// of each sample. Fully generic, so no DE-specific logic lives here.
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
        FactPayloadSnapshot { de: T::DE, schema, samples }
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

    /// Structural guardrail: snapshots the JSON schema of every DE payload. A diff
    /// here means a field/variant/type changed (including an embedded type owned
    /// by another crate).
    #[test]
    fn fact_payload_schemas_are_stable() {
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
                     type; we currently expect DEs to only register one fact type. \
                     Consider using an enum variant.",
                    d.de,
                );
            }
        }
        let serialized = serde_json::to_string_pretty(&doc).unwrap() + "\n";
        expectorate::assert_contents(
            "output/fm_fact_payload_schemas.json",
            &serialized,
        );
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
        expectorate::assert_contents(
            "output/fm_fact_payload_samples.json",
            &serialized,
        );
    }
}
