// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal types for problems.
//!
//! These are *not* part of the external API; for that, see the
//! [`crate::external_api::problem`] module.

use chrono::{DateTime, Utc};
use iddqd::IdOrdMap;
use omicron_common::api::external::Error;
use omicron_generation_kinds::ProblemRevNum;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::ProblemUuid;
use omicron_uuid_kinds::SitrepUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A structured internal representation of problem codes.
///
/// See [§7.2 RFD 699] for details on problem codes.
///
/// This type is intended to be used within the fault management subsystem, such
/// as when creating or updating problems in a sitrep. Using the enum ensures
/// that only defined problem codes are used, and they can be referenced using
/// semantically meaningful names rather than the integer code.
///
/// In the external API, problem codes are represented as a `u32` rather than a
/// `ProblemCode` enum. This type should *not* be included in the external API,
/// since any change to the set of problem codes would therefore require a new
/// API version and machinery between converting between the new and old enums,
/// which is unnecessary given that each problem code has its own versioning
/// information.
///
/// [§7.2 RFD 699]: https://rfd.shared.oxide.computer/rfd/0699#problem-codes
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::VariantArray,
    strum::FromRepr,
    Serialize,
    Deserialize,
)]
#[repr(u32)]
pub enum ProblemCode {
    UnhealthyZpool = 1,
    SmfSvcsInMaintainence = 2,
}

macro_rules! problem_descriptions {
    ($(
        $Variant:ident {
            title: $title:literal,
            class: $class:ident,
            added_in: $added_in:expr,
            retired_in: $retired_in:expr,
        }
    ),+) => {
        impl ProblemCode {
            pub fn description(&self) -> &'static ProblemCodeDescription {
                match self {
                    $(
                        ProblemCode::$Variant => {
                            #[allow(non_upper_case_globals)]
                            const $Variant: &'static ProblemCodeDescription =
                                &ProblemCodeDescription {
                                    title: $title,
                                    class: ProblemClass::$class,
                                    added_in: $added_in,
                                    retired_in: $retired_in,
                                };
                            $Variant
                        }
                    )*
                }
            }
        }
    };
}

problem_descriptions! {
    UnhealthyZpool {
        title: "Unhealthy Zpool",
        class: PhysicalDisk,
        added_in: semver::Version::new(24, 0, 0),
        retired_in: None,
    },
    SmfSvcsInMaintainence {
        title: "Service in Maintainence",
        class: System,
        added_in: semver::Version::new(24, 0, 0),
        retired_in: None,
    }
}

impl ProblemCode {
    pub const ALL: &[ProblemCode] = <Self as strum::VariantArray>::VARIANTS;
}

impl TryFrom<u32> for ProblemCode {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if let Some(code) = ProblemCode::from_repr(value) {
            Ok(code)
        } else {
            Err(Error::InternalError {
                internal_message: format!(
                    "problem code {value} does not exist in the current Oxide \
                    system software version!"
                ),
            })
        }
    }
}

impl From<ProblemCode> for u32 {
    fn from(code: ProblemCode) -> Self {
        code as u32
    }
}

/// A structured internal representation of a problem's class string.
///
/// See [§7.3 RFD 699] for details on problem classes.
///
/// [§7.3 RFD 699]: https://rfd.shared.oxide.computer/rfd/0699#problem-classes
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    strum::EnumString,
    strum::Display,
    strum::IntoStaticStr,
    strum::VariantArray,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
pub enum ProblemClass {
    #[strum(serialize = "hardware.disk")]
    PhysicalDisk,
    #[strum(serialize = "hardware.sled")]
    Sled,
    #[strum(serialize = "hardware.switch")]
    Switch,
    #[strum(serialize = "hardware.power_shelf")]
    PowerShelf,
    #[strum(serialize = "system")]
    System,
    // This list is *incomplete*. You can help by expanding it.
}

/// A static description of a problem code, used to represent the problem code's
/// documentation and other properties.
///
/// See [§7.4 RFD 699] for details on problem documentation.
///
/// [§7.4 RFD 699]: https://rfd.shared.oxide.computer/rfd/0699#oxide-hosted-docs
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProblemCodeDescription {
    /// The [`ProblemClass`] to which all problems with this code belong.
    pub class: ProblemClass,

    /// A one-line, human-readable title for this problem code's documentation.
    pub title: &'static str,

    /// The Oxide system software version in which this problem code was added.
    pub added_in: semver::Version,
    /// The Oxide system software version in which this problem code was
    /// retired, or `None` if it is still active in the current version.
    ///
    /// If this is `Some`, the problem code has been retired and versions later
    /// than this version will no longer report it.
    pub retired_in: Option<semver::Version>,
    // TODO(eliza): more docs sections and stuff!
}

impl ProblemCodeDescription {
    pub fn is_retired(&self) -> bool {
        self.retired_in.is_some()
    }
}

/// A map of all the problems in a sitrep.
///
/// Problems may be looked up by ID, or by their [`ProblemCode`], using the
/// [`AllProblems::by_id`] and [`AllProblems::by_code`] methods, respectively.
#[derive(Clone, Debug, Default)]
pub struct AllProblems {
    by_id: IdOrdMap<Arc<SitrepProblem>>,
    by_code: BTreeMap<ProblemCode, IdOrdMap<Arc<SitrepProblem>>>,
}

impl AllProblems {
    /// Constructs a new `AllProblems` from an iterator of problems.
    ///
    /// This is equivalent to calling [`AllProblems::try_extend`] on an empty
    /// map.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`AllProblems::try_extend`].
    pub fn try_from_iter<I>(
        problems: impl IntoIterator<Item = I>,
    ) -> anyhow::Result<Self>
    where
        SitrepProblem: From<I>,
    {
        let mut this = Self::default();
        this.try_extend(problems)?;
        Ok(this)
    }

    pub fn try_extend<I>(
        &mut self,
        problems: impl IntoIterator<Item = I>,
    ) -> anyhow::Result<&mut Self>
    where
        SitrepProblem: From<I>,
    {
        for problem in problems {
            let problem = SitrepProblem::from(problem);
            // Note: because `try_extend` may have been called with potentially
            // invalid sets of problems in the `Deserialize` impl or when
            // loading a sitrep from the database, we must check here that the
            // problem has at least one revision and reject the problems if it
            // does not. The `SitrepProblem::code()` method will instead assert
            // that there is a problem code, so it should really only be used
            // when the problem has already been constructed and is known to be
            // valid.
            let code =
                problem.revisions.last().map(|rev| rev.code).ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "invalid sitrep: problem {} has no revisions!",
                            problem.id
                        )
                    },
                )?;
            let problem = Arc::new(problem);
            self.by_code
                .entry(problem.code())
                .or_default()
                .insert_unique(problem.clone())
                .map_err(|e| {
                    anyhow::anyhow!(
                        "invalid sitrep: duplicate problem ID in by-code map \
                     for {code:?}: {e}",
                    )
                })?;
            self.by_id.insert_unique(problem).map_err(|e| {
                anyhow::anyhow!(
                    "invalid sitrep: duplicate problem ID in by-id map: {e}",
                )
            })?;
        }
        Ok(self)
    }

    pub fn by_id(&self) -> &IdOrdMap<Arc<SitrepProblem>> {
        &self.by_id
    }

    pub fn by_code(
        &self,
    ) -> &BTreeMap<ProblemCode, IdOrdMap<Arc<SitrepProblem>>> {
        &self.by_code
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

impl<'a> IntoIterator for &'a AllProblems {
    type Item = &'a Arc<SitrepProblem>;
    type IntoIter =
        <&'a IdOrdMap<Arc<SitrepProblem>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.by_id.iter()
    }
}

/// Consumes the map, returning owned problems in ID order.
///
/// This avoids cloning the individual [`Arc`]`<`[`SitrepProblem`]`>`s if they
/// are uniquely owned by this map. If a given problem's [`Arc`] has been cloned
/// out of [`AllProblems::by_id`] or [`AllProblems::by_code`] and the caller is
/// still holding a reference to it, that [`SitrepProblem`] is cloned, instead.
impl IntoIterator for AllProblems {
    type Item = SitrepProblem;
    type IntoIter = std::iter::Map<
        iddqd::id_ord_map::IntoIter<Arc<SitrepProblem>>,
        fn(Arc<SitrepProblem>) -> SitrepProblem,
    >;

    fn into_iter(self) -> Self::IntoIter {
        let Self { by_id, by_code } = self;
        // The by-code index holds a second reference to every problem, so it
        // must be dropped before we drain `by_id`, or else
        // `Arc::unwrap_or_clone` would always clone because `by_id` is not the
        // unique owner of the `Arc`s. Therefore, drop it now
        drop(by_code);
        by_id.into_iter().map(Arc::unwrap_or_clone)
    }
}

impl PartialEq for AllProblems {
    fn eq(&self, other: &Self) -> bool {
        // Note: it is not necessary to compare the by-code maps, because the
        // by-code map is constructed in `AllProblems::try_extend`, and this
        // function will construct the by-code map identically provided that it
        // was constructed from the same set of inputs.
        self.by_id == other.by_id
    }
}

impl Eq for AllProblems {}

/// Serializes as a sequence of [`SitrepProblem`]s, in ID order.
///
/// Only the by-ID map is serialized, as a sequence. The by-code index is
/// derived entirely from the contents of the by-ID map in [`Self::try_extend`],
/// so these types should always round-trip identically without actually
/// serializing both maps.
impl Serialize for AllProblems {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.by_id.iter().map(|problem| &**problem))
    }
}

impl<'de> Deserialize<'de> for AllProblems {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        use serde::de::Error as _;

        let problems = Vec::<SitrepProblem>::deserialize(deserializer)?;
        Self::try_from_iter(problems)
            .map_err(|e| D::Error::custom(format!("{e:#}")))
    }
}

/// A sitrep-internal representation of a problem.
///
/// These records represent a request from the sitrep that a corresponding
/// externally-visible problem record exists. Externally-visible problem records
/// are created by FM rendezvous based on the requested problems in the sitrep
/// being executed.
///
/// A problem is carried forward into subsequent sitreps until *both* the
/// following conditions are true:
///
/// 1. The problem reaches the `resolved` state (i.e., its most recent revision
///    has a non-`None` [`SitrepRevision::time_resolved`]).
/// 2. Rendezvous has created every requested externally visible record
///    associated with that problem.
///
/// Once a problem is resolved, diagnosis engines should not request further
/// revisions of it, but the sitrep must keep tracking it until the externally
/// visible records have been created.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepProblem {
    /// The problem's ID.
    pub id: ProblemUuid,
    /// The sitrep in which a diagnosis engine first requested this problem.
    ///
    /// This is not necessarily the sitrep that was executed when rendezvous
    /// created the externally visible problem, which may be a later one. It
    /// remains the same as the problem is carried forward into subsequent
    /// sitreps.
    pub requested_sitrep_id: SitrepUuid,
    /// A human-readable comment explaining why the diagnosis engine created
    /// this problem.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible problem.
    pub comment: String,
    /// The revisions of this problem, ordered by revision number.
    ///
    /// A problem in a sitrep always has at least one revision, so this should
    /// never be empty.
    pub revisions: IdOrdMap<SitrepRevision>,
    /// Alerts linked to this problem, indexed by alert ID.
    ///
    /// See [`AlertLink`] for details.
    pub alerts: IdOrdMap<AlertLink>,
    /// Support bundles linked to this problem, indexed by support
    /// bundle ID.
    ///
    /// See [`SupportBundleLink`] for details.
    pub support_bundles: IdOrdMap<SupportBundleLink>,
}

impl iddqd::IdOrdItem for SitrepProblem {
    type Key<'a> = &'a ProblemUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

impl SitrepProblem {
    pub fn current_revision(&self) -> &SitrepRevision {
        self.revisions.last().expect(
            "a problem in the sitrep must always have at least one \
            revision!",
        )
    }

    pub fn code(&self) -> ProblemCode {
        self.current_revision().code
    }
}

/// A sitrep-internal representation of a revision of a problem.
///
/// These records represent a request from the sitrep that a corresponding
/// externally-visible problem revision record exists. Externally-visible
/// revision records are created by FM rendezvous based on the requested
/// revisions in the sitrep being executed.
///
/// Revisions are carried forward into subsequent sitreps until the problem has
/// been resolved and rendezvous has created the corresponding
/// externally-visible revision records.
///
/// Diagnosis engines may not request a revision that un-resolves a resolved
/// problem: if a problem's current revision has a `time_resolved`, the sitrep
/// builder should not permit further revisions of that problem.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepRevision {
    /// The revision number.
    ///
    /// A revision's number must always be greater than zero. The zeroth
    /// revision is reserved for a problem that has been created but does not
    /// yet have a current revision.
    pub number: ProblemRevNum,
    /// The sitrep in which a diagnosis engine first requested this revision.
    pub requested_sitrep_id: SitrepUuid,
    /// The time at which the problem was resolved, if it is resolved as of
    /// this revision.
    ///
    /// If this is `None`, the problem is active as of this revision; if it is
    /// `Some`, the problem is resolved.
    pub time_resolved: Option<DateTime<Utc>>,
    /// The problem's problem code as of this revision.
    ///
    /// This determines the problem's class, as well as its documentation.
    pub code: ProblemCode,
    /// A one-sentence, human-readable summary of the problem as of this
    /// revision.
    pub summary: String,
    /// A human-readable description (one or more paragraphs) of the problem as
    /// of this revision.
    pub description: String,
    /// A human-readable description (one or more paragraphs) of any operator
    /// actions required to address the problem.
    pub action_required: String,
    /// A human-readable comment explaining why the diagnosis engine requested
    /// this revision.
    ///
    /// This is used for debugging the sitrep, and is not part of the
    /// externally visible problem revision.
    pub comment: String,
}

impl iddqd::IdOrdItem for SitrepRevision {
    type Key<'a> = &'a ProblemRevNum;
    fn key(&self) -> Self::Key<'_> {
        &self.number
    }

    iddqd::id_upcast!();
}

/// A link between a problem and an alert.
///
/// Linked alerts form a timeline of events over the course of a problem
/// (see [RFD 699][rfd]). An alert may be linked to a problem in the
/// same sitrep that requests the alert, or in any later sitrep, including after
/// the request has been removed from the sitrep. This may occur even if the
/// alert was created prior to the problem. Therefore, a link refers to
/// the alert only by its ID, and does not require that a corresponding
/// [`AlertRequest`](super::case::AlertRequest) exist in the sitrep. The same
/// alert may be linked to multiple problems.
///
/// A link exists for as long as the problem it belongs to remains in the
/// sitrep, and is never removed or modified once created. Therefore,
/// rendezvous can create link records without needing a separate generation
/// counter: as long as the problem itself exists, its links may be created
/// during execution. Once the problem is no longer part of the sitrep *and*
/// rendezvous has created the link, the link need not be carried forward.
///
/// Rendezvous must not create an externally visible link until both the
/// problem and the alert exist. Otherwise, a caller listing a problem's
/// alerts could see alerts that do not exist yet.
///
/// [rfd]: https://rfd.shared.oxide.computer/rfd/0699#alerts-as-timeline
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AlertLink {
    /// The ID of the linked alert.
    pub alert_id: AlertUuid,
    /// The problem revision with which the alert is associated.
    ///
    /// This is used to order a problem's alerts by revision. Like all real
    /// revision numbers, it is greater than zero.
    pub revision: ProblemRevNum,
    /// The sitrep in which this link was first created.
    ///
    /// This is *not* the sitrep that requested the alert, which may be an
    /// earlier one.
    pub linked_sitrep_id: SitrepUuid,
    /// A human-readable comment explaining why the alert is linked to the
    /// problem, for debugging purposes.
    pub comment: String,
}

impl iddqd::IdOrdItem for AlertLink {
    type Key<'a> = &'a AlertUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.alert_id
    }

    iddqd::id_upcast!();
}

/// A link between a problem and a support bundle.
///
/// Like [`AlertLink`]s, support bundle links may be created in the same sitrep
/// that requests the support bundle or in any later sitrep, refer to the
/// support bundle only by its ID, and are never removed or modified while
/// the problem remains in the sitrep. The same support bundle may be
/// linked to multiple problems.
///
/// Once the problem is no longer part of the sitrep *and* rendezvous has
/// created the link, the link need not be carried forward. Rendezvous must not
/// create an externally visible link until both the problem and the support
/// bundle exist, so that a caller listing a problem's support bundles never
/// sees one that does not exist yet.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SupportBundleLink {
    /// The ID of the linked support bundle.
    pub support_bundle_id: SupportBundleUuid,
    /// The problem revision with which the support bundle is associated.
    ///
    /// This is used to order a problem's support bundles by revision. Like all
    /// real revision numbers, it is greater than zero.
    pub revision: ProblemRevNum,
    /// The sitrep in which this link was first created.
    ///
    /// This is *not* the sitrep that requested the support bundle, which may be
    /// an earlier one.
    pub linked_sitrep_id: SitrepUuid,
    /// A human-readable comment explaining why the support bundle is linked
    /// to the problem, for debugging purposes.
    pub comment: String,
}

impl iddqd::IdOrdItem for SupportBundleLink {
    type Key<'a> = &'a SupportBundleUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.support_bundle_id
    }

    iddqd::id_upcast!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use std::collections::BTreeMap;

    fn base_docs_path() -> Utf8PathBuf {
        let mut path = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("docs");
        path.push("problems");
        path
    }

    #[test]
    fn problem_code_docs_exist() {
        let base_path = {
            let mut path = base_docs_path();
            path.push("codes");
            path
        };
        let mut errors = Vec::new();
        let mut paths = BTreeMap::new();
        for problem in ProblemCode::ALL {
            let code = u32::from(*problem);
            let path = base_path.join(format!("{code}.adoc"));
            if !path.exists() {
                errors.push(format!(
                    "error: no documentation file exists for problem code \
                    {problem:?} ({code:04})\n path: {path}",
                ));
            }

            if let Some(prev) = paths.insert(path.clone(), problem) {
                if prev != problem {
                    // this shouldn't happen, but may as well check for it!
                    errors.push(format!(
                        "error: problem code {problem:?} ({code:04}'s docs \
                         path collides with {prev:?} ({:04})\n path: {path}",
                        u32::from(*prev)
                    ));
                }
            }

            // TODO(eliza): we could eventually add some ASCIIDoc linting here.
            // We could either run `asciidoctor -v` (which would require
            // ASCIIDoctor as a build-time dependency, which seems unfortunate),
            // or we could parse the docs using a pure-Rust ASCIIDoc crate, such
            // as https://crates.io/crates/asciidoc-parser (which might be the
            // nicer option).
            //
            // Eventually, we should probably check for the following:
            //
            // - the file contains valid ASCIIDoc syntax
            // - the file contains the expected level-two headings for a problem
            //   code's documentation
            // - the headings have the correct anchors
            // - the file defines the expected attributes that the docs
            //   generator will interpolate.
        }

        if !errors.is_empty() {
            let msg = errors.join("\n");
            panic!(
                "found {} error{} checking problem code documentation:\n{msg}\n\n\
                 if you have added or edited any problem codes, please ensure \
                 that the documentation for these problems exists! see \
                 nexus/types/docs/problems/README.adoc for details \n\
                 on authoring problem documentation.\n",
                errors.len(),
                if errors.len() == 1 { "" } else { "s" }
            )
        }
    }

    /// This test generates the manifest file listing the ASCIIDoc files for
    /// each problem code's documentation and the corresponding metadata.
    ///
    /// When adding new problem codes or modifying their metadata, re-run this
    /// test to ensure that the manifest is updated to contain any new problem
    /// codes you've added or modified.
    #[test]
    fn problem_code_docs_manifest() {
        #[derive(serde::Serialize)]
        struct Entry {
            code: u32,
            description: &'static ProblemCodeDescription,
        }

        let entries = ProblemCode::ALL
            .iter()
            .map(|problem| {
                let code = u32::from(*problem);
                Entry { code, description: problem.description() }
            })
            .collect::<Vec<_>>();
        let manifest = serde_json::to_string_pretty(&entries)
            .expect("manifest will serialize correctly");
        let path = base_docs_path().join("manifest.json");

        expectorate::assert_contents(&path, &manifest)
    }

    fn test_problem(revisions: u32) -> SitrepProblem {
        let sitrep_id = SitrepUuid::new_v4();
        let mut revs = IdOrdMap::new();
        for number in 1..=revisions {
            revs.insert_unique(SitrepRevision {
                number: ProblemRevNum::from_u32(number),
                requested_sitrep_id: sitrep_id,
                time_resolved: None,
                code: ProblemCode::UnhealthyZpool,
                summary: format!("revision {number}"),
                description: format!("description of revision {number}"),
                action_required: "none, it's not a real problem".to_string(),
                comment: format!("revision {number} comment"),
            })
            .expect("revision numbers are distinct");
        }
        let mut alerts = IdOrdMap::new();
        alerts
            .insert_unique(AlertLink {
                alert_id: AlertUuid::new_v4(),
                revision: ProblemRevNum::from_u32(1),
                linked_sitrep_id: sitrep_id,
                comment: "linked alert".to_string(),
            })
            .expect("map is empty");
        let mut support_bundles = IdOrdMap::new();
        support_bundles
            .insert_unique(SupportBundleLink {
                support_bundle_id: SupportBundleUuid::new_v4(),
                revision: ProblemRevNum::from_u32(1),
                linked_sitrep_id: sitrep_id,
                comment: "linked support bundle".to_string(),
            })
            .expect("map is empty");
        SitrepProblem {
            id: ProblemUuid::new_v4(),
            requested_sitrep_id: sitrep_id,
            comment: "test problem".to_string(),
            revisions: revs,
            alerts,
            support_bundles,
        }
    }

    /// Serializing and deserializing an `AllProblems` must reproduce both the
    /// by-ID map and the by-code index, even though it only serializes the
    /// by-ID map.
    #[test]
    fn all_problems_serde_roundtrip() {
        let mut problems = AllProblems::default();
        problems
            .try_extend([test_problem(1), test_problem(2), test_problem(3)])
            .expect("problems are valid");

        let json =
            serde_json::to_value(&problems).expect("problems should serialize");
        let serialized = json.as_array().unwrap_or_else(|| {
            panic!("problems should serialize as a sequence: {json:#}")
        });
        assert_eq!(serialized.len(), problems.len());

        let deserialized: AllProblems = serde_json::from_value(json)
            .expect("serialized problems should deserialize");
        assert_eq!(deserialized, problems);
        // `PartialEq` for `AllProblems` only compares the by-ID maps, so
        // check that the by-code index was rebuilt separately.
        assert_eq!(deserialized.by_code(), problems.by_code());
    }

    #[test]
    fn all_problems_deserialize_rejects_duplicate_ids() {
        let problem = test_problem(1);
        let json = serde_json::to_string(&[&problem, &problem])
            .expect("problems should serialize");
        let err = serde_json::from_str::<AllProblems>(&json)
            .expect_err("duplicate problem IDs should be rejected");
        assert!(
            err.to_string().contains("duplicate problem ID"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn all_problems_deserialize_rejects_empty_revisions() {
        let problem = test_problem(0);
        let json = serde_json::to_string(&[&problem])
            .expect("problems should serialize");
        let err = serde_json::from_str::<AllProblems>(&json)
            .expect_err("a problem with no revisions should be rejected");
        assert!(
            err.to_string().contains("has no revisions"),
            "unexpected error: {err}",
        );
    }
}
