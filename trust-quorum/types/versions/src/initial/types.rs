// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Basic trust quorum types.

use daft::Diffable;
use derive_more::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A unique, sequentially increasing identifier for a configuration.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    Diffable,
    JsonSchema,
)]
#[daft(leaf)]
#[schemars(transparent)]
pub struct Epoch(pub u64);

/// The number of shares required to reconstruct the rack secret.
///
/// Typically referred to as `k` in the docs.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Display,
    Diffable,
    JsonSchema,
)]
#[daft(leaf)]
#[schemars(transparent)]
pub struct Threshold(pub u8);
