// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal types for Active Problems.
//!
//! These are *not* part of the external API; for that, see the
//! [`crate::external_api::active_problem`] module.

use omicron_common::api::external::Error;

/// A structured internal representation of problem codes.
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

#[derive(Clone, Debug, serde::Serialize)]
pub struct ProblemCodeDescription {
    pub title: &'static str,
    pub class: ProblemClass,
    pub description: &'static str,
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
