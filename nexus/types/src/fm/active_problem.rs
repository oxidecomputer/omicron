// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal types for Active Problems.
//!
//! These are *not* part of the external API; for that, see the
//! [`crate::external_api::active_problem`] module.

use omicron_common::api::external::Error;

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
    Copy, Clone, Debug, PartialEq, Eq, strum::VariantArray, strum::FromRepr,
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

    // TODO(eliza): there should be a way to squirt out the problem as ASCIIDoc
    // or something for use on the docs site!
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8Path;
    use std::collections::BTreeMap;

    #[test]
    fn problem_code_docs_exist() {
        let mut errors = Vec::new();
        let mut paths = BTreeMap::new();
        let docs_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("docs/active-problems/problems");
        for problem in ProblemCode::ALL {
            let code = u32::from(*problem);
            let path = docs_dir.join(format!("{code:04}.adoc"));
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

            // TODO(eliza): we could eventually add an `asciidoctor -v` check
            // here, but that means adding ASCIIDoctor as a build dependency...
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

    #[test]
    fn problem_code_description_manifest() {
        #[derive(serde::Serialize)]
        struct Entry {
            code: u32,
            description: &'static ProblemCodeDescription,
        }

        let entries: Vec<Entry> = ProblemCode::ALL
            .iter()
            .map(|problem| Entry {
                code: u32::from(*problem),
                description: problem.description(),
            })
            .collect();

        let json = serde_json::to_string_pretty(&entries).unwrap();
        expectorate::assert_contents(
            "docs/active-problems/problems/manifest.json",
            &json,
        );
    }
}
