// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal types for Active Problems.
//!
//! These are *not* part of the external API; for that, see the
//! [`crate::external_api::active_problem`] module.

use iddqd::IdOrdMap;
use omicron_common::api::external::Error;
use omicron_generation_kinds::ActiveProblemRevNum;
use omicron_uuid_kinds::ActiveProblemUuid;
use omicron_uuid_kinds::SitrepUuid;
use serde::{Deserialize, Serialize};

/// A structured internal representation of problem codes.
///
/// See [§7.2 RFD 699] for details on problem codes.
///
/// This type is intended to be used within the fault management subsystem, such
/// as when creating or updating Active Problems in a sitrep. Using the enum
/// ensures that only defined problem codes are used, and they can be referenced
/// using semantically meaningful names rather than the integer code.
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
    strum::VariantArray,
    strum::FromRepr,
    Serialize,
    Deserialize,
)]
#[repr(u32)]
pub enum ProblemCode {
    // Unfortunately necessary because you can't have a `repr(...)` attribute on
    // an enum with no variants! Sigh.
    NotARealProblem = 0,
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
    NotARealProblem {
        title: "Not a Real Problem",
        class: System,
        added_in: semver::Version::new(24, 0, 0),
        retired_in: Some(semver::Version::new(24, 0, 0)),
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

/// A structured internal representation of an Active Problem's class string.
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
/// See [§7.4 RFD 699] for details on Active Problem documentation.
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

/// The sitrep internal representation of an Active Problem.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepActiveProblem {
    pub id: ActiveProblemUuid,
    pub requested_sitrep_id: SitrepUuid,
    pub revisions: IdOrdMap<SitrepRevision>,
}

impl iddqd::IdOrdItem for SitrepActiveProblem {
    type Key<'a> = &'a ActiveProblemUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.id
    }

    iddqd::id_upcast!();
}

impl SitrepActiveProblem {
    pub fn current_revision(&self) -> &SitrepRevision {
        self.revisions.last().expect(
            "an active problem in the sitrep must always have at least one \
            revision!",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SitrepRevision {
    pub number: ActiveProblemRevNum,
    pub requested_sitrep_id: SitrepUuid,
    pub code: ProblemCode,
    pub summary: String,
    pub description: String,
    pub action_required: String,
}

impl iddqd::IdOrdItem for SitrepRevision {
    type Key<'a> = &'a ActiveProblemRevNum;
    fn key(&self) -> Self::Key<'_> {
        &self.number
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
        path.push("active-problems");
        path
    }

    #[test]
    fn problem_code_docs_exist() {
        let base_path = {
            let mut path = base_docs_path();
            path.push("problems");
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
                 nexus/types/docs/active-problems/README.adoc for details \n\
                 on authoring Active Problem documentation.\n",
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
}
