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

#[derive(Clone, Copy, Debug)]
pub enum CpuFamily {
    Unknown,
    AmdMilan,
    AmdTurin,
}
