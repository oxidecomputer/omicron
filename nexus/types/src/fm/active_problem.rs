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
/// # Adding New Problem Codes
///
/// In order to define a new problem code, add a new variant to this `enum`.
/// This will require adding a new match arm to [`ProblemCode::description()`]
/// for the new variant, and in turn, defining a [`ProblemCodeDescription`] for
/// the the newly-defined code. The [`ProblemCodeDescription`] struct defines
/// documentation for each problem code and other properties (such as the
/// problem's class) which are determined by the problem code.
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

impl ProblemCode {
    pub fn description(&self) -> &'static ProblemCodeDescription {
        match self {
            ProblemCode::NotARealProblem => {
                const NOT_A_REAL_PROBLEM: &'static ProblemCodeDescription =
                    &ProblemCodeDescription {
                        title: "Not a Real Problem",
                        class: ProblemClass::System,
                        description: "This is not a real problem. It only \
                             exists due to an unfortunate Rust language \
                             quirk and can be safely ignored.",
                        action_required: "If you encounter this problem in \
                             real life, this is a bug. Please contact Oxide \
                             support. But, everything is fine and no other \
                             action is required.",
                        added_in: semver::Version::new(24, 0, 0),
                        retired_in: Some(semver::Version::new(24, 0, 0)),
                    };
                &NOT_A_REAL_PROBLEM
            }
        }
    }
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
    /// A human-readable description, potentially spanning multiple paragraphs,
    /// which describes the general nature of the problem.
    pub description: &'static str,
    /// A human-readable description of any actions an operator must perform to
    /// resolve this problem.
    ///
    /// Depending on the nature of the problem, the "action required" may be
    /// "contact Oxide support" or "do nothing and wait for automated response
    /// to complete". q
    pub action_required: &'static str,

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
