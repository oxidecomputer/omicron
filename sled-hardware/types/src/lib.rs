// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod underlay;

/// Describes properties that should uniquely identify a Gimlet.
#[derive(
    Clone,
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Baseboard {
    Gimlet { identifier: String, model: String, revision: u32 },

    Unknown,

    Pc { identifier: String, model: String },
}

impl Baseboard {
    #[allow(dead_code)]
    pub fn new_gimlet(
        identifier: String,
        model: String,
        revision: u32,
    ) -> Self {
        Self::Gimlet { identifier, model, revision }
    }

    pub fn new_pc(identifier: String, model: String) -> Self {
        Self::Pc { identifier, model }
    }

    // XXX This should be removed, but it requires a refactor in how devices are
    // polled.
    pub fn unknown() -> Self {
        Self::Unknown
    }

    pub fn type_string(&self) -> &str {
        match &self {
            Self::Gimlet { .. } => "gimlet",
            Self::Pc { .. } => "pc",
            Self::Unknown => "unknown",
        }
    }

    pub fn identifier(&self) -> &str {
        match &self {
            Self::Gimlet { identifier, .. } => &identifier,
            Self::Pc { identifier, .. } => &identifier,
            Self::Unknown => "unknown",
        }
    }

    pub fn model(&self) -> &str {
        match self {
            Self::Gimlet { model, .. } => &model,
            Self::Pc { model, .. } => &model,
            Self::Unknown => "unknown",
        }
    }

    pub fn revision(&self) -> u32 {
        match self {
            Self::Gimlet { revision, .. } => *revision,
            Self::Pc { .. } => 0,
            Self::Unknown => 0,
        }
    }
}

impl std::fmt::Display for Baseboard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Baseboard::Gimlet { identifier, model, revision } => {
                write!(f, "gimlet-{identifier}-{model}-{revision}")
            }
            Baseboard::Unknown => write!(f, "unknown"),
            Baseboard::Pc { identifier, model } => {
                write!(f, "pc-{identifier}-{model}")
            }
        }
    }
}

/// Identifies the kind of CPU present on a sled, determined by reading CPUID.
///
/// This is intended to broadly support the control plane answering the question
/// "can I run this instance on that sled?" given an instance with either no or
/// some CPU platform requirement. It is not enough information for more precise
/// placement questions - for example, is a CPU a high-frequency part or
/// many-core part? We don't include Genoa here, but in that CPU family there
/// are high frequency parts, many-core parts, and large-cache parts. To support
/// those questions (or satisfactorily answer #8730) we would need to collect
/// additional information and send it along.
#[derive(
    Serialize, Deserialize, Copy, Clone, Debug, PartialEq, Eq, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SledCpuFamily {
    /// The CPU vendor or its family number don't correspond to any of the
    /// known family variants.
    Unknown,

    /// AMD Milan processors (or very close). Could be an actual Milan in a
    /// Gimlet, a close-to-Milan client Zen 3 part, or Zen 4 (for which Milan is
    /// the greatest common denominator).
    AmdMilan,

    /// AMD Turin processors (or very close). Could be an actual Turin in a
    /// Cosmo, or a close-to-Turin client Zen 5 part.
    AmdTurin,

    /// AMD Turin Dense processors. There are no "Turin Dense-like" CPUs unlike
    /// other cases, so this means a bona fide Zen 5c Turin Dense part.
    AmdTurinDense,
}

impl SledCpuFamily {
    fn as_str(&self) -> &'static str {
        match self {
            SledCpuFamily::Unknown => "unknown",
            SledCpuFamily::AmdMilan => "amd_milan",
            SledCpuFamily::AmdTurin => "amd_turin",
            SledCpuFamily::AmdTurinDense => "amd_turin_dense",
        }
    }
}

impl std::fmt::Display for SledCpuFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
