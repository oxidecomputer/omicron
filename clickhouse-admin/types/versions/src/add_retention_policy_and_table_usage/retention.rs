// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU8;

/// Policy for retaining telemetry data.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct RetentionPolicy {
    /// The retention period, in days.
    pub days: Days,
}

/// A number of days used for a retention period.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[schemars(transparent, range(min = 1, max = "MAX_DAYS"))]
pub struct Days(NonZeroU8);

impl From<Days> for u8 {
    fn from(val: Days) -> u8 {
        val.0.get()
    }
}

const MAX_DAYS: u8 = 30;

impl Days {
    /// Return a new number of days.
    ///
    /// The argument must be within [1, 30].
    pub const fn new(val: u8) -> Option<Self> {
        if val > MAX_DAYS {
            None
        } else {
            match NonZeroU8::new(val) {
                Some(v) => Some(Self(v)),
                None => None,
            }
        }
    }
}
